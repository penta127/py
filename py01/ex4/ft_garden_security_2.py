
class SecurePlant:
    def __init__(self,name):
        self._name = name
        self._height = 0
        self._age = 0

    def set_height(self, height):
        if height < 0:
            print(f"Invalid operation attempted: height {height}cm [REJECTED]")
            print("Security: Negative height rejected")
            return
        self.height = height
        print(f"Height updated: {height}cm [OK]")

    def set_age(self, age)
        if age < 0:
            print(f"Invalid operation attempted: age {age}days [REJECTED]")
            print("")