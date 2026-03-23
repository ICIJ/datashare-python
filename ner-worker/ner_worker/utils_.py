def lower_camel_to_snake_case(s: str) -> str:
    snake = "".join("_" + c.lower() if c.isupper() else c for c in s)
    if snake.startswith("_"):
        snake = snake[1:]
    return snake
