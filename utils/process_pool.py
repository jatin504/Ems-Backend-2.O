from concurrent.futures import ProcessPoolExecutor

# Global process pool
PROCESS_POOL = ProcessPoolExecutor(max_workers=1)