"""
Google Play Store Feasibility Test
Scrapes ChatGPT app reviews to evaluate data structure and quality

Author: Lile Zhang
Date: January 2026
"""

import csv

from google_play_scraper import Sort, app, reviews, reviews_all


def get_chatgpt_app_info(app_id="com.openai.chatgpt"):
    """
    Get basic information about the ChatGPT app
    """
    print("=" * 50)
    print("1. Fetching ChatGPT App Information")
    print("=" * 50)

    try:

        result = app(app_id, lang="en", country="us")

        # print(json.dumps(result, indent=2)) # Full app info

        print(f"App Title: {result['title']}")
        print(f"Developer: {result['developer']}")
        print(f"Category: {result['genre']}")
        print(f"Rating: {result['score']} ({result['ratings']} ratings)")
        print(f"Reviews Count: {result['reviews']}")
        print(f"Installs: {result['realInstalls']}")
        print(f"Release Date: {result['released']}")
        print(f"Last Updated: {result['lastUpdatedOn']}")

        return result

    except Exception as e:
        print(f"Error Fetching info: {e}")
        return None


def fetch_reviews(
    app_id="com.openai.chatgpt", batch_size=200, total_count=1000, all_reviews=False
):
    """
    Fetch all or partial reviews
    """
    print("\n" + "=" * 50)

    try:
        # Fetch all reviews (not working for data source with large volume of reviews)
        if all_reviews:
            print(f"2: Fetching All Reviews")
            result = reviews_all(
                app_id,
                sleep_milliseconds=0,
                lang="en",
                country="us",
                sort=Sort.NEWEST,
            )
            print(f"Successfully fetched all reviews: {len(result)} reviews")

        else:
            print(f"2: Fetching {total_count} Reviews")
            all_reviews = []
            continuation_token = None

            while len(all_reviews) < total_count:
                try:
                    if continuation_token:
                        batch_reviews, continuation_token = reviews(
                            app_id,
                            lang="en",
                            country="us",
                            sort=Sort.NEWEST,
                            count=batch_size,
                            continuation_token=continuation_token,
                        )
                    else:
                        batch_reviews, continuation_token = reviews(
                            app_id,
                            lang="en",
                            country="us",
                            sort=Sort.NEWEST,
                            count=batch_size,
                        )
                    all_reviews.extend(batch_reviews)

                    if continuation_token is None:
                        print("No more reviews to fetch.")
                        break
                except Exception as e:
                    print(f"Error fetching batch of reviews: {e}")
                    break

        print("=" * 50)
        return all_reviews

    except Exception as e:
        print(f"Error fetching reviews: {e}")
        return None


def analyze_review_rating_distribution(reviews):
    """
    Analyze the distribution of review ratings
    """
    print("\n" + "=" * 50)
    print("3: Analyzing Review Rating Distribution")
    print("=" * 50)

    rating_distribution = {i: 0 for i in range(1, 6)}

    for review in reviews:
        rating = review.get("score", 0)
        if rating in rating_distribution:
            rating_distribution[rating] += 1

    for rating, count in rating_distribution.items():
        print(f"Rating {rating}: {count} reviews, {count / len(reviews) * 100:.1f}%")

    return rating_distribution


def analyze_review_content_length(reviews):
    """
    Analyze the length of review content
    """
    print("\n" + "=" * 50)
    print("4: Analyzing Review Content Length")
    print("=" * 50)

    lengths = [len(review.get("content", "")) for review in reviews]
    average_length = sum(lengths) / len(lengths) if lengths else 0
    max_length = max(lengths) if lengths else 0
    min_length = min(lengths) if lengths else 0

    print(f"Average Review Length: {average_length:.2f} characters")
    print(f"Max Review Length: {max_length} characters")
    print(f"Min Review Length: {min_length} characters")

    return {
        "average_length": average_length,
        "max_length": max_length,
        "min_length": min_length,
    }


def analyze_review_date_distribution(reviews):
    """
    Inspect the distribution of review dates
    """
    print("\n" + "=" * 50)
    print("Analyzing Review Date Distribution")
    print("=" * 50)

    date_distribution = {}

    for review in reviews:
        review_date = review.get("at")
        if review_date:
            date_str = review_date.strftime("%Y-%m-%d")
            if date_str in date_distribution:
                date_distribution[date_str] += 1
            else:
                date_distribution[date_str] = 1

    sorted_dates = sorted(date_distribution.items())

    for date_str, count in sorted_dates:
        print(f"Date {date_str}: {count} reviews")

    return date_distribution


def display_sample_reviews(reviews, sample_size=3):
    """
    Display a few sample reviews for inspection
    """
    print("\n" + "=" * 50)
    print(f"5: Displaying Sample Reviews")
    print("=" * 50)

    for i, review in enumerate(reviews[:sample_size]):
        print(f"\nReview {i + 1}:")
        print(f"User: {review.get('userName')}")
        print(f"Rating: {review.get('score')}")
        print(f"Date: {review.get('at')}")
        print(f"Content: {review.get('content')}")


def save_reviews_to_csv(reviews, filename="data/raw/chatgpt_reviews.csv"):
    """
    Save reviews to a CSV file
    """

    print("\n" + "=" * 50)
    print(f"Saving Reviews to {filename}")
    print("=" * 50)

    keys = reviews[0].keys() if reviews else []
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(reviews)

    print(f"Successfully saved {len(reviews)} reviews to {filename}")


def save_product_info_to_csv(product_info, filename="data/raw/chatgpt_app_info.csv"):
    """
    Save product info to a CSV file
    """

    print("\n" + "=" * 50)
    print(f"Saving Product Info to {filename}")
    print("=" * 50)

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=product_info.keys())
        writer.writeheader()
        writer.writerow(product_info)

    print(f"Successfully saved product info to {filename}")


def main():
    app_info = get_chatgpt_app_info()
    save_product_info_to_csv(app_info)

    sample_reviews = fetch_reviews(total_count=50000)
    analyze_review_rating_distribution(sample_reviews)
    analyze_review_content_length(sample_reviews)
    analyze_review_date_distribution(sample_reviews)
    save_reviews_to_csv(sample_reviews)
    display_sample_reviews(sample_reviews, sample_size=3)


if __name__ == "__main__":
    main()
