import math

def position_created():
    text = "10,20,5"
    parts = text.split(",")
    x = int(parts[0])
    y = int(parts[1])
    z = int(parts[2])

    pos = (x, y, z)
    origin = (0, 0, 0)

    x1, y1, z1 = origin
    x2, y2, z2 = pos
    result = math.sqrt((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2)

    print(f"Position created: {pos}")
    print(f"Distance between {origin} and {pos}: {result:.2f}")


def parsing_coor():
    text = "3,4,0"
    parts = text.split(",")
    x = int(parts[0])
    y = int(parts[1])
    z = int(parts[2])

    pos = (x, y, z)
    origin = (0, 0, 0)

    x1, y1, z1 = origin
    x2, y2, z2 = pos
    result = math.sqrt((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2)

    print('Parsing coordinates: "3,4,0"')
    print(f"Parsed position: {pos}")
    print(f"Distance between {origin} and {pos}: {result}")
    return pos

def parsing_error():
    try:
        text = "abc,def,ghi"
        parts = text.split(",")
        x = int(parts[0])
        y = int(parts[1])
        z = int(parts[2])
    except ValueError as e:
        print('Parsing invalid coordinates: "abc,def,ghi"')
        print(f"Error parsing coordinates: {e}")
        print(f"Error details - Type: {type(e).__name__}, Args: {e.args}")


def unpacking_demo(pos):
    x, y, z = pos

    print("Unpacking demonstration:")
    print(f"Player at x={x}, y={y}, z={z}")
    print(f"Coordinates: X={x}, Y={y}, Z={z}")



def ft_coordinate_system():
    print("=== Game Coordinate System ===")
    print()
    position_created()
    print()
    pos = parsing_coor()
    print()
    parsing_error()
    print()
    unpacking_demo(pos)

if __name__ == "__main__":
    ft_coordinate_system()