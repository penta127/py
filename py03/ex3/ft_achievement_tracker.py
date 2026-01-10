
def ft_achievement_tracker():
    alice = set(("first_kill", "level_10", "treasure_hunter", "speed_demon"))
    bob = set(("first_kill", "level_10", "boss_slayer", "collector"))
    charlie = set(("level_10", "treasure_hunter", "boss_slayer", "speed_demon", "perfectionist"))

    all_unique = alice.union(bob).union(charlie)
    common_to_all = alice.intersection(bob).intersection(charlie)

    rare_alice = alice.difference(bob.union(charlie))
    rare_bob = bob.difference(alice.union(charlie))
    rare_charlie = charlie.difference(alice.union(bob))
    rare_all = rare_alice.union(rare_bob).union(rare_charlie)



    print("=== Achievement Tracker System ===")
    print()
    print(f"Player alice achievements: {alice}")
    print(f"Player bob achievements: {bob}")
    print(f"Player charlie achievements: {charlie}")
    print()
    print("=== Achievement Analytics ===")
    print(f"All unique achievements: {all_unique}")
    print(f"Total unique achievements: {len(all_unique)}")
    print()
    print(f"Common to all players: {common_to_all}")
    print(f"Rare achievements (1 player): {rare_all}")
    print()
    print(f"Alice vs Bob common: {alice.intersection(bob)}")
    print(f"Alice unique: {alice.difference(bob)}")
    print(f"Bob unique: {bob.difference(alice)}")


if __name__ == "__main__":
    ft_achievement_tracker()