import sys

def ft_score_analytics():
    print("=== Player Score Analytics ===")
    if len(sys.argv) == 1:
        print("No scores provided. Usage: python3 ft_score_analytics.py <score1> <score2> ...")
        return
    try:
        scores = []
        for i in range(1, len(sys.argv)):
            scores.append(int(sys.argv[i]))
        print(f"Scores processed: {scores}")
        print(f"Total players: {len(scores)}")
        print(f"Total score: {sum(scores)}")
        print(f"Average score: {sum(scores) / len(scores)}")
        print(f"High score: {max(scores)}")
        print(f"Low score: {min(scores)}")
        print(f"Score range: {max(scores) - min(scores)}")
        print()

    except ValueError:
        print("No scores provided. Usage: NO INT!!")
        return
    
if __name__ == "__main__":
    ft_score_analytics()