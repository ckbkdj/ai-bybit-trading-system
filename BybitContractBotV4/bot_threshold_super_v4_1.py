"""Active execution entrypoint.

Prediction interpretation, risk, sizing, exchange I/O and recovery live in dedicated
modules. Historical strategy files remain read-only for comparison.
"""

from service_main import run_service


def main() -> None:
    run_service()


if __name__ == "__main__":
    main()
