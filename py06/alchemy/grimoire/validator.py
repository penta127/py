
def validate_ingredients(ingredients: str) -> str:

    txt = ingredients.split()
    validates = ["fire", "water", "earth", "air"]

    if all(t in validates for t in txt):
        return f"{ingredients} - VALID"
    else:
        return f"{ingredients} - INVALID"
