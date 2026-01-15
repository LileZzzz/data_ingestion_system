"""
Exploratory Data Analysis - ChatGPT Reviews (50K Dataset)

Author: Lile Zhang
Date: January 2026
"""

from collections import Counter
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats


def load_data(filepath="data/raw/chatgpt_reviews_recent_50K.csv"):
    """Load the 50K reviews dataset"""
    print("=" * 80)
    print("LOADING DATA")
    print("=" * 80)

    df = pd.read_csv(filepath)

    print(f"Loaded {len(df):,} reviews")
    print(f"Columns: {list(df.columns)}")
    return df


def basic_statistics(df):
    """Basic dataset statistics"""
    print("\n" + "=" * 80)
    print("1. BASIC STATISTICS")
    print("=" * 80)

    print(f"\nDataset Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    print("\nColumns and Data Types:")
    for col, dtype in df.dtypes.items():
        non_null = df[col].notna().sum()
        null_pct = (1 - non_null / len(df)) * 100
        print(
            f"  {col:30} {str(dtype):15} {non_null:>6,} non-null ({null_pct:>5.1f}% missing)"
        )

    print("\nBasic Statistics:")
    print(df.describe())


def analyze_rating_distribution(df):
    """
    Rating Distribution
    """
    print("\n" + "=" * 80)
    print("2. RATING DISTRIBUTION ANALYSIS")
    print("=" * 80)

    rating_counts = df["score"].value_counts().sort_index()

    print("\nRating Distribution:")
    print("-" * 80)
    for rating, count in rating_counts.items():
        pct = (count / len(df)) * 100
        print(
            f" {int(rating)} star{'s' if rating != 1 else ' '}: {count:>6,} ({pct:>5.1f}%)"
        )

    # Statistical measures
    print("\nStatistical Measures:")
    print(f"  Mean Rating:   {df['score'].mean():.2f}")
    print(f"  Median Rating: {df['score'].median():.1f}")
    print(f"  Mode Rating:   {df['score'].mode()[0]:.0f}")
    print(f"  Std Dev:       {df['score'].std():.2f}")

    # Rating skewness
    skewness = stats.skew(df["score"])
    print(f"  Skewness:      {skewness:.2f}", end="")
    if skewness < -0.5:
        print(" (Negatively skewed - more high ratings)")
    elif skewness > 0.5:
        print(" (Positively skewed - more low ratings)")
    else:
        print(" (Relatively balanced)")

    # Positive vs Negative
    positive = len(df[df["score"] >= 4])
    negative = len(df[df["score"] <= 2])
    neutral = len(df[df["score"] == 3])

    print(f"\nSentiment Breakdown:")
    print(f"  Positive (4-5 stars): {positive:>6,} ({positive/len(df)*100:>5.1f}%)")
    print(f"  Neutral  (3 stars):   {neutral:>6,} ({neutral/len(df)*100:>5.1f}%)")
    print(f"  Negative (1-2 stars): {negative:>6,} ({negative/len(df)*100:>5.1f}%)")

    return rating_counts


def analyze_text_length(df):
    """
    Text Length Analysis
    """
    print("\n" + "=" * 80)
    print("3. TEXT LENGTH ANALYSIS")
    print("=" * 80)

    # Calculate lengths
    df["word_count"] = df["content"].fillna("").str.split().str.len()

    print("\nWord Count Statistics:")
    print("-" * 80)
    print(f"  Mean:       {df['word_count'].mean():>8.1f} words")
    print(f"  Median:     {df['word_count'].median():>8.0f} words")
    print(f"  Min:        {df['word_count'].min():>8.0f} words")
    print(f"  Max:        {df['word_count'].max():>8,.0f} words")

    # Length distribution
    length_counts = df["word_count"].value_counts().sort_index()
    print("\nWord Count Distribution (top 10):")
    print("-" * 80)
    for length, count in length_counts.head(10).items():
        pct = (count / len(df)) * 100
        print(f" {length:>4} words: {count:>6,} ({pct:>5.1f}%)")

    return df[["word_count"]]


def analyze_language_mix(df):
    """
    Language Mix Analysis
    """
    print("\n" + "=" * 80)
    print("4. LANGUAGE MIX ANALYSIS")
    print("=" * 80)

    # Simple language detection based on character sets
    def detect_language_simple(text):
        if pd.isna(text) or text == "":
            return "empty"

        # Count different character types
        ascii_count = sum(1 for c in text if ord(c) < 128)
        chinese_count = sum(1 for c in text if "\u4e00" <= c <= "\u9fff")
        cyrillic_count = sum(1 for c in text if "\u0400" <= c <= "\u04ff")
        arabic_count = sum(1 for c in text if "\u0600" <= c <= "\u06ff")

        total = len(text)
        if total == 0:
            return "empty"

        # Determine dominant language
        if chinese_count / total > 0.3:
            return "chinese"
        elif cyrillic_count / total > 0.3:
            return "cyrillic"
        elif arabic_count / total > 0.3:
            return "arabic"
        elif ascii_count / total > 0.8:
            return "english"
        else:
            return "mixed"

    print("\nDetecting languages in sample of 5000 reviews...")
    sample = df.sample(min(5000, len(df)))
    sample["detected_language"] = sample["content"].apply(detect_language_simple)

    lang_counts = sample["detected_language"].value_counts()

    print("\nLanguage Distribution (sample):")
    print("-" * 80)
    for lang, count in lang_counts.items():
        pct = (count / len(sample)) * 100
        print(f"  {lang.capitalize():15} {count:>5,} ({pct:>5.1f}%)")

    # Emoji analysis
    def has_emoji(text):
        if pd.isna(text):
            return False
        # Simple emoji detection
        emoji_pattern = r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF]"
        import re

        return bool(re.search(emoji_pattern, text))

    emoji_count = sample["content"].apply(has_emoji).sum()

    print(f"\nEmoji Usage:")
    print(
        f"  Reviews with emojis: {emoji_count:,} ({emoji_count/len(sample)*100:.1f}%)"
    )

    return lang_counts


def analyze_temporal_patterns(df):
    """
    Temporal Patterns Analysis
    """
    print("\n" + "=" * 80)
    print("5. TEMPORAL PATTERNS ANALYSIS")
    print("=" * 80)

    # Convert to datetime
    if "at" in df.columns:
        df["review_date"] = pd.to_datetime(df["at"], errors="coerce")
    elif "reviewed_at" in df.columns:
        df["review_date"] = pd.to_datetime(df["reviewed_at"], errors="coerce")
    else:
        print("\nNo timestamp column found")
        return

    # Remove invalid dates
    valid_dates = df["review_date"].notna()
    print(
        f"\n✓ Valid timestamps: {valid_dates.sum():,} / {len(df):,} ({valid_dates.sum()/len(df)*100:.1f}%)"
    )

    df_temporal = df[valid_dates].copy()

    if len(df_temporal) == 0:
        print("\nNo valid timestamps to analyze")
        return

    # Date range
    print(f"\nDate Range:")
    print(f"  Earliest: {df_temporal['review_date'].min()}")
    print(f"  Latest:   {df_temporal['review_date'].max()}")
    print(
        f"  Span:     {(df_temporal['review_date'].max() - df_temporal['review_date'].min()).days} days"
    )

    # Reviews per day
    df_temporal["date"] = df_temporal["review_date"].dt.date
    daily_counts = df_temporal.groupby("date").size()

    print(f"\nReviews per Day:")
    print(f"  Mean:     {daily_counts.mean():>8.1f} reviews/day")
    print(f"  Median:   {daily_counts.median():>8.0f} reviews/day")
    print(f"  Min:      {daily_counts.min():>8,} reviews/day")
    print(f"  Max:      {daily_counts.max():>8,} reviews/day")

    # Day of week pattern
    df_temporal["day_of_week"] = df_temporal["review_date"].dt.day_name()
    dow_counts = df_temporal["day_of_week"].value_counts()

    print(f"\nReviews by Day of Week:")
    day_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    for day in day_order:
        if day in dow_counts:
            count = dow_counts[day]
            pct = (count / len(df_temporal)) * 100
            print(f"  {day:10} {count:>6,} ({pct:>5.1f}%)")

    # Hour pattern
    if df_temporal["review_date"].dt.hour.notna().any():
        df_temporal["hour"] = df_temporal["review_date"].dt.hour
        hour_counts = df_temporal["hour"].value_counts().sort_index()

        print(f"\nReviews by Hour of Day (top 5):")
        for hour, count in hour_counts.head(5).items():
            pct = (count / len(df_temporal)) * 100
            print(f"  {int(hour):02d}:00  {count:>6,} ({pct:>5.1f}%)")

    return df_temporal


def analyze_missing_fields(df):
    """
    Missing and Noisy Fields Analysis
    """
    print("\n" + "=" * 80)
    print("6. MISSING & NOISY FIELDS ANALYSIS")
    print("=" * 80)

    print("\nField Completeness:")
    print("-" * 80)

    for col in df.columns:
        non_null = df[col].notna().sum()
        null_count = len(df) - non_null
        null_pct = (null_count / len(df)) * 100

        print(
            f" {col:30} {non_null:>6,} / {len(df):>6,} ({100-null_pct:>5.1f}% complete)"
        )

    # Identify critical vs non-critical missing fields
    critical_fields = ["reviewId", "content", "score"]
    non_critical_fields = ["reviewCreatedVersion", "replyContent", "repliedAt"]

    print(f"\nCritical Fields (must have):")
    for field in critical_fields:
        if field in df.columns:
            completeness = (df[field].notna().sum() / len(df)) * 100
            status = "✓" if completeness == 100 else "❌"
            print(f"  {status} {field:30} {completeness:>5.1f}% complete")

    print(f"\nNon-Critical Fields:")
    for field in non_critical_fields:
        if field in df.columns:
            completeness = (df[field].notna().sum() / len(df)) * 100
            print(f"     {field:30} {completeness:>5.1f}% complete")

    # Data quality checks
    print(f"\nData Quality Checks:")

    # Check for duplicate IDs
    if "reviewId" in df.columns:
        duplicates = df["reviewId"].duplicated().sum()
        print(f"  Duplicate review IDs: {duplicates:,}")

    # Check for invalid scores
    if "score" in df.columns:
        invalid_scores = len(df[(df["score"] < 1) | (df["score"] > 5)])
        print(f"  Invalid scores (not 1-5): {invalid_scores:,}")

    # Check for empty content
    if "content" in df.columns:
        empty_content = len(df[df["content"].fillna("").str.strip() == ""])
        print(f"  Empty content: {empty_content:,} ({empty_content/len(df)*100:.1f}%)")


def main():
    print("\n" + "=" * 80)
    print("EXPLORATORY DATA ANALYSIS")
    print("ChatGPT Reviews - 50K Dataset")
    print("=" * 80)

    df = load_data("data/raw/chatgpt_reviews_recent_50K.csv")
    basic_statistics(df)
    analyze_rating_distribution(df)
    analyze_text_length(df)
    analyze_language_mix(df)
    analyze_temporal_patterns(df)
    analyze_missing_fields(df)


if __name__ == "__main__":
    main()
