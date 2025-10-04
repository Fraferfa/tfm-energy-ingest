# Local copy so that when you bind mount the repo over /app you still have the dispatcher.
from docker.entrypoint import main

if __name__ == "__main__":
    main()
