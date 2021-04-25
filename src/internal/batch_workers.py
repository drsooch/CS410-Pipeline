num_batches = 5


def set_batch_number(batches: int) -> None:
    global num_batches

    if type(batches) is int:
        num_batches = batches
    else:
        raise ValueError(
            f"Number of batches should be an Integer, received {batches}={type(batches)}"
        )


def get_batch_number() -> int:
    return num_batches
