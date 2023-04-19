import random


def get_random_sleep(rnd_min: float = 0.5, rnd_max: float = 1.0) -> float:
    return random.uniform(rnd_min, rnd_max)  # Random in [min .. max]
