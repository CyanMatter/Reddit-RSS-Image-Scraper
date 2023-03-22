import logging
from logging.handlers import TimedRotatingFileHandler
import traceback
from bs4 import BeautifulSoup, SoupStrainer
import asyncio
import httpx
import re
from collections import namedtuple
from urllib.parse import urlencode, urlparse, urlunparse
from math import ceil
from os import makedirs, getcwd, path, remove
import sys
from time import time
from tqdm import tqdm
import utils

VER = '1.0'
COMPONENTS = namedtuple(
    typename='Components',
    field_names=['scheme', 'netloc', 'path', 'params', 'query', 'fragment']
)
G_PARAMS = {
    'ref_source': ['embed'],
    'ref': ['share'],
    'embed': ['true']
}
G_STRAINER = SoupStrainer('div', {'class': 'gallery-preview'})
SUBREDDIT_TITLE_PATTERN = re.compile('\/?r\/([a-z0-9][_a-z0-9]{2,20})(?:\Z|\s)', flags=re.M | re.I)
LOGGER = logging.getLogger(__name__)
LOGGER_LEVEL = logging.DEBUG

MAX_REQUEST_RETRIES = 3
LIMIT_SET = True
IMAGE_SET_SIZE_LIMIT = 30
DOWNLOAD_DIRECTORY = r'C:\\Users\\Public\\Pictures\Reddit RSS Image Scraper\\'
TQDM_BAR_FORMAT = '{desc:<24.23}{percentage:3.0f}%|{bar:25}{r_bar}'


class ImageData:
    caption = number = title = author = sub = content = url = file = None

    def __str__(self):
        return f'''caption: {self.caption}
number:  {self.number}
title:   {self.title}
author:  {self.author}
sub:     {self.sub}
content: {self.content}
url:     {self.url}
file:    {self.file}
'''

    def copy(self):
        new = ImageData()
        new.caption = self.caption
        new.number = self.number
        new.title = self.title
        new.author = self.author
        new.sub = self.sub
        new.content = self.content
        new.url = self.url
        new.file = self.file
        return new


# Server connection
def parse_response(resp, strainer=None):
    contenttype = resp.headers['content-type']

    if contenttype.startswith('application/atom+xml'):
        parser = 'xml'
    elif contenttype.startswith(('text/html', 'text/plain')):
        parser = 'html.parser'
    else:
        raise ValueError('''\
            The body of the response fetched from {} is in an unsupported \
            form: '{}'. Supported content-types are exclusively \
            'text/html,' 'text/plain,' and 'application/atom+xml.'\
            '''.format(resp.url, contenttype))

    if strainer is not None:
        return BeautifulSoup(resp.content, parser, parse_only=strainer)
    return BeautifulSoup(resp.content, parser)


async def log_request(req):
    LOGGER.info(f'Request \t{req.method} {req.url}\t- Awaiting response')


async def log_response(resp):
    req = resp.request
    LOGGER.info(f'Response\t{req.method} {req.url}\t- Status {resp.status_code} {resp.reason_phrase}')


async def backoff_and_retry(resp, client, strainer, retries, log_msg, MAX_REQUEST_RETRIES):
    req = resp.request
    if retries < MAX_REQUEST_RETRIES:
        backoff = 3 if 'retry-after' not in resp.headers else int(resp.headers['retry-after'])
        log_msg += f'\nMaking attempt {retries + 1}/{MAX_REQUEST_RETRIES} for {req.method} {req.url} after {backoff}s.'
        LOGGER.warning(log_msg)
        return await send_and_await(client, strainer, MAX_REQUEST_RETRIES, req=req, backoff=backoff,
                                    retries=retries + 1)
    else:
        LOGGER.error(f'Gave up on {req.method} {req.url}: retry limit reached.')
        return resp


async def handle_http_error(resp, client, strainer, retries, MAX_REQUEST_RETRIES):
    if resp.status_code == 408:
        log_msg = '''\
The server has closed the connection because the request reached its timeout limit.
This could be due to heavy traffic at the server, or due to your network connection being throttled.\
'''
        return await backoff_and_retry(resp, client, strainer, retries, log_msg, MAX_REQUEST_RETRIES)
    elif resp.status_code == 429:
        log_msg = 'The server indicated that it received too many requests.'
        return await backoff_and_retry(resp, client, strainer, retries, log_msg, MAX_REQUEST_RETRIES)
    elif resp.status_code == 500:
        log_msg = '''\
The server encountered an unexpected condition that prevented it from fulfilling the request.\
'''
        return await backoff_and_retry(resp, client, strainer, retries, log_msg, MAX_REQUEST_RETRIES)
    else:
        return resp


async def send_and_await(client, strainer, MAX_REQUEST_RETRIES, path=None, req=None, backoff=0, retries=1):
    await asyncio.sleep(backoff)

    if req is not None:
        resp = await client.send(req)
    elif path is not None:
        resp = await client.get(path)
    else:
        raise ValueError('Function \'send_and_await\' expects either argument \'p\' or \'req\' to not be NoneType.')

    if resp.is_error:
        return await handle_http_error(resp, client, strainer, retries, MAX_REQUEST_RETRIES)
    else:
        return parse_response(resp, strainer)


async def request_and_parse(base_url, MAX_REQUEST_RETRIES, paths=None, params=None, strainer=None):
    event_hooks = {'request': [log_request], 'response': [log_response]}
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    transport = httpx.AsyncHTTPTransport(retries=1)
    async with httpx.AsyncClient(base_url=base_url,
                                 params=params,
                                 event_hooks=event_hooks,
                                 limits=limits,
                                 transport=transport
                                 ) as client:
        if paths is not None:
            tasks = [asyncio.create_task(send_and_await(client, strainer, MAX_REQUEST_RETRIES, path=path)) for path in
                     paths]
            return await asyncio.gather(*tasks)
        else:
            req = httpx.Request('GET', base_url, params=params)
            return await send_and_await(client, strainer, MAX_REQUEST_RETRIES, req=req)


async def get_entries(url, MAX_REQUEST_RETRIES):
    soup = await request_and_parse(url, MAX_REQUEST_RETRIES, strainer=SoupStrainer('entry'))
    entries = "".join([entry.contents[0] for entry in soup.findAll('content')])
    return BeautifulSoup(entries, 'html.parser')


# Discovery
def find_all_solos(doc):
    hyperlinks = doc.find_all(href=re.compile('^https:\/\/i\.(redd\.it|imgur\.com)\/'))

    sol_entries = []
    for link in hyperlinks:
        row = link.find_parent('tr')
        if row is not None:
            sol_entries.append(row)

    return sol_entries


def find_all_galleries(doc):
    hyperlinks = doc.find_all(href=re.compile('^https:\/\/www\.reddit\.com\/gallery\/'))

    g_entries = []
    for link in hyperlinks:
        row = link.find_parent('tr')
        if row is not None:
            g_entries.append(row)

    g_ids = [str(link['href'])[31:] for link in hyperlinks]
    return g_entries, g_ids


def get_all_title_author_url(orig_data, entries, is_sol=False):
    data_lst = []
    for entry in entries:
        data = orig_data

        data.title = entry.a.img['title']
        data.url = entry.a['href']
        data.author = entry.td.next_sibling.a.next_element[4:]
        if is_sol:
            data.content = entry.td.next_sibling.span.a['href']

        data_lst.append(data.copy())
    return data_lst


def parse_gallery(soup, gal_data):
    num = 1
    imgs = []
    img = soup.div
    while (img is not None):
        img_data = gal_data

        div = img.find('div', {'class': 'gallery-item-caption'})

        if div is not None:
            img_data.caption = div.next_element.replace('Caption: ', '').strip()
            img_data.number = None
        else:
            img_data.caption = gal_data.title
            img_data.number = num
            num += 1

        preview_url = img.find('a', {'class': 'may-blank gallery-item-thumbnail-link'})['href']
        img_id_ext = urlparse(preview_url).path
        img_data.content = 'https://i.redd.it' + img_id_ext

        imgs.append(img_data.copy())

        img = img.next_sibling

    return imgs


def create_g_urls(sub, *g_ids):
    query = {'ref_source': 'embed',
             'ref': 'share',
             'embed': 'true'
             }
    return [urlunparse(
        COMPONENTS(
            scheme='https',
            netloc='www.redditmedia.com',
            path=f'r/{sub}/comments/{g_id}/',
            query=urlencode(query),
            params='',
            fragment=''
        )
    ) for g_id in g_ids]


async def discover_galleries(doc, data, MAX_REQUEST_RETRIES):
    base_url = f'https://www.redditmedia.com/r/{data.sub}/comments'

    g_entries, g_ids = find_all_galleries(doc)
    g_soups = await request_and_parse(base_url, MAX_REQUEST_RETRIES, paths=g_ids, params=G_PARAMS, strainer=G_STRAINER)
    g_data_lst = get_all_title_author_url(data, g_entries)

    imgs = []
    for g_soup, g_data in zip(g_soups, g_data_lst):
        imgs.extend(parse_gallery(g_soup, g_data))

    return imgs


def discover_solos(doc, data):
    sol_entries = find_all_solos(doc)
    return get_all_title_author_url(data, sol_entries, is_sol=True)


async def discover_images_for_url(entries, data_stub: ImageData, url: str, MAX_REQUEST_RETRIES: int):
    imgs = await discover_galleries(entries, data_stub, MAX_REQUEST_RETRIES)
    imgs.extend(discover_solos(entries, data_stub))

    for img in imgs:
        img.file = create_filename(img)

    log_msg = f'Discovered {len(imgs)} images at {url}.'
    print(log_msg)
    LOGGER.info(log_msg)

    return imgs


async def discover_images(urls: list[str], data_stubs: list[ImageData], MAX_REQUEST_RETRIES) -> list[list[ImageData]]:
    img_groups = []
    for url, data_stub in zip(urls, data_stubs):
        entries = await get_entries(url, MAX_REQUEST_RETRIES)
        img_groups.append(await discover_images_for_url(entries, data_stub, url, MAX_REQUEST_RETRIES))
    return img_groups


# Filesystem
def create_filename(img):
    # Format: time-this_is_a_caption_abbrevia...-by_user.extension 
    if img.number is not None:
        number = f'({str(img.number)})'
    else:
        number = ''

    parts = {
        'epoch': str(int(time())),
        'user': as_filename('_'.join(['by', img.author])),
        'extension': path.splitext(img.content)[1],
        'number': number
    }

    # Truncate caption if the entire filename is over 125 characters long
    # The limit 125 is arbitrarily chosen: 125 is the line width of GitHub's diff view.
    remaining_space = 123 - sum(len(parts[key]) for key in parts)

    if img.caption is not None:
        caption = as_filename(img.caption)
    else:
        caption = as_filename(img.title)

    if len(caption) > remaining_space:
        caption = truncate(caption, remaining_space)
    parts['caption'] = caption

    return '{epoch}-{caption}{number}-{user}{extension}'.format(**parts)


def as_filename(string):
    string = string.replace(' ', '_')
    string = ''.join(x.lower() for x in string if is_valid_char(x))
    return string


def is_valid_char(x):
    return x == '_' or x.isalnum() or x == '-'


def truncate(string, end_at):
    end_at = 3 if end_at < 3 else end_at
    return string[:end_at - 3].rstrip('_') + '...'


def total_length(groups: list[list]) -> int:
    return sum(len(group) for group in groups)


def limit_per_url(imgs: list[list[ImageData]], IMAGE_SET_SIZE_LIMIT) -> list[list[ImageData]]:
    if total_length(imgs) <= IMAGE_SET_SIZE_LIMIT:
        return imgs

    imgs.sort(key=len)
    return limit_image_sets(imgs, IMAGE_SET_SIZE_LIMIT)


def limit_image_sets(img_groups: list[list[ImageData]], limit: int) -> list[list[ImageData]]:
    if total_length(img_groups) <= limit:
        return img_groups

    smallest_set = img_groups[0]
    avg = limit // len(img_groups)
    limited_sets = [smallest_set[:avg]]
    if len(img_groups) == 1:
        return limited_sets
    else:
        to_limit = limit - ceil(limit / len(img_groups))
        limited_sets.extend(limit_image_sets(img_groups[1:], to_limit))
        return limited_sets


def download(img: ImageData, folder, progress):
    loc = path.join(folder, img.file)
    with open(loc, 'wb') as f:
        with httpx.stream('GET', img.content) as response:
            total = int(response.headers['content-length'])

            progress.reset()
            progress.total = total
            progress.desc = img.caption if img.caption is not None else img.title

            num_bytes_downloaded = response.num_bytes_downloaded
            for chunk in response.iter_bytes():
                f.write(chunk)
                progress.update(response.num_bytes_downloaded - num_bytes_downloaded)
                num_bytes_downloaded = response.num_bytes_downloaded
    LOGGER.info(f'Saved {img.content} at {loc}.')


def download_all(img_groups: list[list[ImageData]] | None, out_dir: str, TQDM_BAR_FORMAT):
    if img_groups is None:
        return

    makedirs(out_dir, exist_ok=True)

    with tqdm(position=1,
              desc='Image set',
              total=len(img_groups),
              unit='image',
              ascii=True,
              dynamic_ncols=True,
              bar_format=TQDM_BAR_FORMAT) as total_progress:
        with tqdm(position=0,
                  unit_scale=True,
                  unit_divisor=1024,
                  unit='B',
                  ascii=True,
                  dynamic_ncols=True,
                  bar_format=TQDM_BAR_FORMAT) as download_progress:
            for group in img_groups:
                for img in group:
                    download(img, out_dir, download_progress)
                    total_progress.update()


def is_valid_input_and_yield_sub(path: str):
    match = re.search(SUBREDDIT_TITLE_PATTERN, path)
    return match.group(1)


def resolve_input(input: str):
    parts = urlparse(input)
    sub = is_valid_input_and_yield_sub(parts.path)

    if sub is None:
        raise ValueError(f'The input {input} could not be resolved as the name of a subreddit.')

    data_stub = ImageData()
    data_stub.sub = sub
    return f'https://www.reddit.com/r/{sub}.rss', data_stub


def load_sources():
    sources_file = 'subreddits.txt'
    utils.file_exists_in_cwd(sources_file)

    urls = []
    data_stubs = []
    with open(sources_file) as sources:
        for line in sources.readlines():
            if line.startswith('#'):
                continue
            url, data_stub = resolve_input(line)
            urls.append(url)
            data_stubs.append(data_stub)

    plural = 's' if len(urls) != 1 else ''
    LOGGER.info(f'Loaded {len(urls)} source{plural}: {urls}.')
    return urls, data_stubs


def setup_logger() -> str:
    dir = getcwd() + '\\logs'
    makedirs(dir, exist_ok=True)

    name = f'crash-{str(int(time()))}.log'
    loc = path.join(dir, name)
    handler = TimedRotatingFileHandler(filename=loc,
                                       when='D',
                                       interval=1,
                                       backupCount=12,
                                       encoding='utf-8',
                                       delay=False)

    formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')

    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
    LOGGER.setLevel(LOGGER_LEVEL)

    return loc


def close_logger(success: bool):
    logging.shutdown()
    if success and (LOGGER_LEVEL != logging.DEBUG):
        remove(path_log)


if __name__ == "__main__":

    path_log = setup_logger()
    try:
        urls, data_stubs = load_sources()

        # Discovers all information about images found in each url.
        imgs = asyncio.run(discover_images(urls, data_stubs, MAX_REQUEST_RETRIES))

        # Get a subset of images if its size exceeds the limit, and LIMIT_SET is set.
        if LIMIT_SET:
            imgs = limit_per_url(imgs, IMAGE_SET_SIZE_LIMIT)

        # Downloads each image in the ImageData list to out_dir directory.
        download_all(imgs, DOWNLOAD_DIRECTORY, TQDM_BAR_FORMAT)

    except Exception as e:
        LOGGER.error(traceback.format_exc())
        print(f'Error! Check the crash log at {path_log} for more info.')
        close_logger(False)
        sys.exit()

    print(f'Saved {total_length(imgs)} images at {DOWNLOAD_DIRECTORY}.')
    close_logger(True)
