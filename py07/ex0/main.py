from .Card import Card
from .CreatureCard import CreatureCard


def main() -> None:
    print()
    print("=== DataDeck Card Foundation ===")
    print()
    print("Testing Abstract Base Class Design:")
    print()
    
    creature = CreatureCard('Fire Dragon')
    print("CreatureCard Info:")
    print(f"{creature.get_card_info()}")



if __name__ == "__main__":
    main()