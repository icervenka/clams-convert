def regular_freq(delta):
    if delta.days > 0:
        raise UserWarning("Interval seems to be too long, is there a parsing error?")
    interval = delta.total_seconds()
    return str(interval) + 'S'

