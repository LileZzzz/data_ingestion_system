# Work Log / Learning Journal

## 2025 - 12 - 30 Week

- Primary data source (Google Play ChatGPT [1])
- Google Play scraper documentation (google-play-scraper [2])
    
    - Installation: 

            pip install google-play-scraper

    - Access:        
        - App details
        - App reviews (Setting *count* too high can **cause** problems)
        - App all reviews
        - App permissions, search
- Test:
    - Fetch app details
        - Most relevant information:
            - App title, developer, cateogry, average rating, rating count, reviews count, installs, release and last updated date
    - Fetch app sample reviews (via google-play-scraper's *reviews*)
        - **Not now** (but later will) observe any issues with high count (test scraping 100K reviews (set *count* as 100K) - takes 2 min)
        - Analyze the rating distribution (Newest 100K reviews)
            | Rating | Count | Percentage|
            |-----:|---------:|-----:|
            | ⭐ 1 | 7,556 | 7.56% |
            | ⭐⭐ 2 | 1,963 | 1.96% |
            | ⭐⭐⭐ 3 | 4,246 | 4.25% |
            | ⭐⭐⭐⭐ 4 | 9,970 | 9.97% |
            | ⭐⭐⭐⭐⭐ 5 | 76,265 | 76.27% |
        - Analyze the rating content (Newest 100K reviews)
            | Content Stats | Length |
            |:---------|-------:|
            | Average Length | 33.46 |
            | Max Length | 500 |
            | Min Length | 1 |
        - Fetch the most recent 50k data to inspect data flow per day (~2.7k/day - Very decent amount of data flow)
            | Date | Review Count |
            |:---------|--------:|
            | 2025-12-19 | 2,333 |
            | 2025-12-20 | 2,731 |
            | 2025-12-21 | 2,967 |
            | 2025-12-22 | 2,797 |
            | 2025-12-23 | 2,724 |
            | 2025-12-24 | 2,594 |
            | 2025-12-25 | 2,594 |
            | 2025-12-26 | 2,607 |
            | 2025-12-27 | 2,622 |
            | 2025-12-28 | 2,752 |
            | 2025-12-29 | 2,717 |
            | 2025-12-30 | 2,735 |
            | 2025-12-31 | 2,743 |
            | 2026-01-01 | 2,649 |
            | 2026-01-02 | 2,981 |
            | 2026-01-03 | 3,051 |
            | 2026-01-04 | 3,255 |
            | 2026-01-05 | 3,148 |

        
    - Fetch and analyze all reviews (via google-play-scraper's *reviews_all*):
        - Do not work for ChatGPT reviews (time out - probably due to high volume of reviews since it works for the data source with a total of 16K reviews). Think this shouldn't be a problem as we have 2.7k new data incoming every day and we still can fetch historical data progressively (via *reviews*) instead of one shot.

    - Save the raw data
        - Use .csv for now
        - Observed the issues: All user names are "A Google user" （[`chatgpt_reviews_recent_50k_missing_user_name.csv`](./data/raw/chatgpt_reviews_recent_50k_missing_user_name.csv)）
            - Figured it out: *count* (I call it *batch_size*) cannot be set over 2K - use 200 to be safe
            - Fixed the function - use *batch_size* for number of review per crawling, *total_count* for total number of reviews to fetch
        - Result [`chatgpt_reviews_recent_50k.csv`](./data/raw/chatgpt_reviews_recent_50k.csv)）
            - Observed mixed languages, emojis, symbols in the reviews 
- Structure the pipeline (Working on it)
- Schema desing (Working on it)
    

[1]: https://play.google.com/store/apps/details?id=com.openai.chatgpt&hl=en&gl=us

[2]: https://pypi.org/project/google-play-scraper/

