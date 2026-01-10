

def gene_eve(num):
    for i in range(num):
        yield i

def ft_data_stream():
    print("=== Game Data Stream Processor ===")
    print()
    print("Processing 1000 game events...")
    print()
    processed = 0
    for i in gene_eve(1000):
        processed += 1
        if processed == 1:
            print("Event 1: Player alice (level 5) killed monster")
        elif processed == 2:
            print("Event 2: Player bob (level 12) found treasure")
        elif processed == 3:
            print("Event 3: Player charlie (level 8) leveled up")
        
    print("...")
    print()
    print("=== Stream Analytics ===")
    print(f"Total events processed: {processed}")


if __name__ == "__main__":
    ft_data_stream()