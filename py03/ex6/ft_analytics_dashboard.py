def ft_analytics_dashboard():
    players = [
        {"name": "alice", "score": 2300, "active": True,  "region": "north",
        "achievements": ["first_kill", "level_10"]},
        {"name": "bob", "score": 1800, "active": True,  "region": "east",
        "achievements": ["first_kill"]},
        {"name": "charlie", "score": 2150, "active": True, "region": "central",
        "achievements": ["level_10", "boss_slayer"]},
        {"name": "diana", "score": 2050, "active": False, "region": "north",
        "achievements": ["first_kill", "boss_slayer"]},
    ]

    print("=== Game Analytics Dashboard ===")
    print()
    print("=== List Comprehension Examples ===")
    high_scorers = [p["name"] for p in players if p["score"] > 2000]
    scores_doubled = [p["score"] * 2 for p in players]
    active_players = [p["name"] for p in players if p["active"]]

    print(f"High scorers (>2000): {high_scorers}")
    print(f"Scores doubled: {scores_doubled}")
    print(f"Active players: {active_players}")
    print()
    print("=== Dict Comprehension Examples ===")
    scores = [p["score"] for p in players]
    player_scores = {p["name"]: p["score"] for p in players}

    score_categories = {
        "high": sum([1 for s in scores if s > 2000]),
        "medium": sum([1 for s in scores if 1000 < s <= 2000]),
        "low": sum([1 for s in scores if s <= 1000]),
    }

    achievement_counts = {p["name"]: len(p["achievements"]) for p in players}

    print(f"Player scores: {player_scores}")
    print(f"Score categories: {score_categories}")
    print(f"Achievement counts: {achievement_counts}")
    print()
    print("=== Set Comprehension Examples ===")
    unique_players = {p["name"] for p in players}
    unique_achievements = {a for p in players for a in p["achievements"]}
    active_regions = {p["region"] for p in players if p["active"]}

    print(f"Unique players: {unique_players}")
    print(f"Unique achievements: {unique_achievements}")
    print(f"Active regions: {active_regions}")
    print()
    print("=== Combined Analysis ===")
    total_players = len(unique_players)
    total_unique_achievements = len(unique_achievements)
    average_score = sum(scores) / len(scores) if len(scores) else 0

    top_score, top_name = max([(p["score"], p["name"]) for p in players])
    top_ach = achievement_counts[top_name]

    print(f"Total players: {total_players}")
    print(f"Total unique achievements: {total_unique_achievements}")
    print(f"Average score: {average_score}")
    print(f"Top performer: {top_name} ({top_score} points, {top_ach} achievements)")


if __name__ == "__main__":
    ft_analytics_dashboard()
