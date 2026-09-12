def run_func(func, *args, **kwargs):
    from datetime import datetime
    from tqdm import tqdm
    start_time = datetime.now()

    func(*args, **kwargs)

    end_time = datetime.now()

    duration = (end_time - start_time).total_seconds()

    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)

    tqdm.write(
        f"Duration: {int(hours):02d}:"
        f"{int(minutes):02d}:"
        f"{seconds:06.3f}"
    )