import time

def gene_eve(num):
    players = ["alice", "bob", "charlie"]
    kinds = ["killed monster", "found treasure", "leveled up"]

    for i in range(num):
        player = players[i % len(players)]
        level = (i % 20) + 1            
        kind = kinds[i % len(kinds)]     
        yield player, level, kind

def fibonacci_wr():
    a = 0
    b = 1
    while True:
        yield a
        a, b = b, a + b


def primes_wr():
    n = 2
    while True:
        is_prime = True
        for d in range(2, n):
            if n % d == 0:
                is_prime = False
                break
        if is_prime:
            yield n
        n += 1

def first_n_str(gen_func, n):
    it = iter(gen_func())
    out = ""
    for i in range(n):
        val = next(it)
        if i > 0:
            out += ", "
        out += str(val)
    return out

def ft_data_stream():
    start = time.time()
    print("=== Game Data Stream Processor ===")
    print()
    print("Processing 1000 game events...")
    print()

    processed = 0
    high_level = 0
    treasure_events = 0
    levelup_events = 0

    for player, level, kind in gene_eve(1000):
        processed += 1

        if level >= 10:
            high_level += 1
        if kind == "found treasure":
            treasure_events += 1
        if kind == "leveled up":
            levelup_events += 1

        if processed <= 3:
            print(f"Event {processed}: Player {player} (level {level}) {kind}")
    end = time.time()

    print("...")
    print()
    print("=== Stream Analytics ===")
    print(f"Total events processed: {processed}")
    print(f"High-level players (10+): {high_level}")
    print(f"Treasure events: {treasure_events}")
    print(f"Level-up events: {levelup_events}")
    print()
    print("Memory usage: Constant (streaming)")
    print(f"Processing time: {end - start :.3f} seconds")
    print()
    print("=== Generator Demonstration ===")
    print("=== Generator Demonstration ===")
    print("Fibonacci sequence (first 10): " + first_n_str(fibonacci_wr, 10))
    print("Prime numbers (first 5): " + first_n_str(primes_wr, 5))

    

if __name__ == "__main__":
    ft_data_stream()
