import os
import random
import string

from locust import HttpUser
from locust import between
from locust import task


def random_value(length: int = 10) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))


class UserBehavior(HttpUser):
    wait_time = between(1, 3)
    host = os.getenv("LOCUST_HOST", "http://localhost:8000")

    def on_start(self) -> None:
        self.hot_keys = ["alpha", "beta", "gamma", "delta", "omega"]
        for key in self.hot_keys:
            self.client.put(
                f"/seed/{key}",
                json={"value": f"warm-{key}"},
                name="/seed/[warm]",
            )

    @task(3)
    def data(self) -> None:
        key = random.choice(self.hot_keys)
        self.client.get(f"/data?key={key}", name="/data")

    @task(2)
    def login(self) -> None:
        user = random.choice(["user:123", "user:456", "feature:search"])
        self.client.get(f"/login?user={user}", name="/login")

    @task(2)
    def purchase(self) -> None:
        order_id = f"order-{random_value(8)}"
        self.client.get(f"/purchase?order_id={order_id}", name="/purchase")

    @task(1)
    def notify(self) -> None:
        message_id = f"msg-{random_value(8)}"
        self.client.get(f"/notify?message_id={message_id}", name="/notify")

    @task(1)
    def mixed(self) -> None:
        choice = random.choice(
            [
                lambda: self.client.get(f"/data?key={random.choice(self.hot_keys)}", name="/data[mix]"),
                lambda: self.client.get(f"/login?user={random.choice(['user:123', 'user:456'])}", name="/login[mix]"),
                lambda: self.client.get(f"/purchase?order_id=order-{random_value(6)}", name="/purchase[mix]"),
                lambda: self.client.get(f"/notify?message_id=msg-{random_value(6)}", name="/notify[mix]"),
            ]
        )
        choice()
