"""Create a tiny deterministic Instacart-shaped dataset for an offline smoke test."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def write_csv(path: Path, header: list[str], rows: list[tuple]) -> None:
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(header)
        writer.writerows(rows)


def make_fixture(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    orders, prior, train = [], [], []
    order_id = 1
    for user_id in range(1, 61):
        seen = set()
        for number in range(1, 5):
            eval_set = "prior" if number < 4 else ("train" if user_id <= 50 else "test")
            orders.append((order_id, user_id, eval_set, number, user_id % 7,
                           8 + user_id % 12, "" if number == 1 else 7))
            basket = [(user_id % 5) + 1, ((user_id + number) % 10) + 6,
                      ((user_id * 3 + number) % 10) + 6]
            if number == 4:
                basket = [basket[0], ((user_id + 3) % 10) + 6,
                          ((user_id * 3 + 1) % 10) + 6]
            for position, product_id in enumerate(dict.fromkeys(basket), start=1):
                row = (order_id, product_id, position, int(product_id in seen))
                if eval_set == "prior":
                    prior.append(row)
                elif eval_set == "train":
                    train.append(row)
                seen.add(product_id)
            order_id += 1
    write_csv(root / "orders.csv", ["order_id", "user_id", "eval_set", "order_number",
                                    "order_dow", "order_hour_of_day", "days_since_prior_order"], orders)
    header = ["order_id", "product_id", "add_to_cart_order", "reordered"]
    write_csv(root / "order_products__prior.csv", header, prior)
    write_csv(root / "order_products__train.csv", header, train)
    write_csv(root / "products.csv", ["product_id", "product_name", "aisle_id", "department_id"],
              [(i, f"Product {i}", 1 if i <= 5 else 2, 1) for i in range(1, 16)])
    write_csv(root / "aisles.csv", ["aisle_id", "aisle"], [(1, "frequent"), (2, "other")])
    write_csv(root / "departments.csv", ["department_id", "department"], [(1, "grocery")])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/fixture"))
    make_fixture(parser.parse_args().output)
