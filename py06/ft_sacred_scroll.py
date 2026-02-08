import alchemy
import alchemy.elements

def main() -> None:
    print()
    print("=== Sacred Scroll Mastery ===")
    print()
    direct = [
        alchemy.elements.create_fire,
        alchemy.elements.create_water,
        alchemy.elements.create_earth,
        alchemy.elements.create_air,
    ]
    print("Testing direct module access:")
    for create in direct:
        print(f"alchemy.elements.{create.__name__}(): {create()}")
    print()
    print("Testing package-level access (controlled by __init__.py):")

    try:
        print(f"alchemy.create_fire(): {alchemy.create_fire()}")
    except AttributeError:
        print(f"alchemy.create_fire(): AttributeError - not exposed")

    try:
        print(f"alchemy.create_water(): {alchemy.create_water()}")
    except AttributeError:
        print(f"alchemy.create_water(): AttributeError - not exposed")

    try:
        print(f"alchemy.create_earth(): {alchemy.create_earth()}")
    except AttributeError:
        print("alchemy.create_earth(): AttributeError - not exposed")


    try:
        print(f"alchemy.create_air(): {alchemy.create_air()}")
    except AttributeError:
        print(f"alchemy.create_air(): AttributeError - not exposed")

    print()
    print("Package metadata:")
    print(f"Version: {alchemy.__version__}")
    print(f"Author: {alchemy.__author__}")
    
if __name__ == "__main__":
    main()