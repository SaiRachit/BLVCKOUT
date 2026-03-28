import math


class OnlineStats:
    def __init__(self) -> None:
        self.count = 0
        self.mean_val = 0.0
        self._m2 = 0.0

    def push(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean_val
        self.mean_val += delta / self.count
        delta2 = value - self.mean_val
        self._m2 += delta * delta2

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        return self._m2 / (self.count - 1)

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)

    def z_score(self, value: float) -> float:
        stddev = self.stddev
        if self.count < 2 or stddev == 0.0:
            return 0.0
        return abs(value - self.mean_val) / stddev


class ExponentialMovingAverage:
    def __init__(self, alpha: float) -> None:
        self.alpha = alpha
        self.value = 0.0
        self._initialized = False

    def update(self, sample: float) -> float:
        if not self._initialized:
            self.value = sample
            self._initialized = True
            return self.value
        self.value = self.alpha * sample + (1.0 - self.alpha) * self.value
        return self.value
