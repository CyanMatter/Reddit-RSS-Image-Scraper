# Reddit-RSS-Image-Scraper
This Python script checks the RSS feeds of a list of subreddits for images. These images are currently published on the subreddits' front pages. It will then download these images to `C:\Users\Public\Pictures\Reddit RSS Image Scraper`.

# Settings
Subreddits to download from can be listed in subreddits.txt.
The program limits itself to a total of 30 downloads. You can edit this number by changing `IMAGE_SET_SIZE_LIMIT` in `main.py`. Alternatively, you can remove the limit altogether by setting `LIMIT_SET` to `FALSE` in the same file.