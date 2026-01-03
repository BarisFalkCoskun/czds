#!/usr/bin/env python3
"""
ICANN CZDS Zone File Downloader

Downloads all approved zone files from ICANN's Centralized Zone Data Service.

Setup:
    pip install czds-api
    export CZDS_USER='your_email@example.com'
    export CZDS_PASS='your_password'

Usage:
    python download_zones.py
"""

import asyncio
import os
import sys
from datetime import datetime

try:
    from czds import CZDS
    import aiofiles
    from tqdm import tqdm
except ImportError:
    print("Required packages not installed. Run: pip install czds-api")
    sys.exit(1)


async def download_zone_compressed(session, headers, url: str, output_directory: str, semaphore: asyncio.Semaphore):
    """Download a single zone file without decompressing it."""

    async def _download():
        tld_name = url.split('/')[-1].split('.')[0]
        max_retries = 20
        retry_delay = 5

        download_headers = {
            **headers,
            'Connection': 'keep-alive',
            'Keep-Alive': 'timeout=600',
            'Accept-Encoding': 'gzip'
        }

        for attempt in range(max_retries):
            try:
                async with session.get(url, headers=download_headers) as response:
                    if response.status != 200:
                        if attempt + 1 < max_retries:
                            await asyncio.sleep(retry_delay)
                            continue
                        raise Exception(f'Failed to download {tld_name}: {response.status}')

                    expected_size = int(response.headers.get('Content-Length', 0))
                    content_disposition = response.headers.get('Content-Disposition')

                    if not content_disposition:
                        raise ValueError(f'Missing Content-Disposition header for {tld_name}')

                    filename = content_disposition.split('filename=')[-1].strip('"')
                    filepath = os.path.join(output_directory, filename)

                    with tqdm(total=expected_size, unit='B', unit_scale=True, desc=f'Downloading {tld_name}', leave=False) as pbar:
                        async with aiofiles.open(filepath, 'wb') as file:
                            total_size = 0
                            async for chunk in response.content.iter_chunked(8192):
                                await file.write(chunk)
                                total_size += len(chunk)
                                pbar.update(len(chunk))

                    if expected_size and total_size != expected_size:
                        os.remove(filepath)
                        if attempt + 1 < max_retries:
                            await asyncio.sleep(retry_delay)
                            continue
                        raise Exception(f'Incomplete download for {tld_name}')

                    return filepath

            except Exception:
                if attempt + 1 >= max_retries:
                    raise
                await asyncio.sleep(retry_delay)

    async with semaphore:
        return await _download()


async def main():
    username = os.environ.get("CZDS_USER", "")
    password = os.environ.get("CZDS_PASS", "")
    date_folder = datetime.now().strftime("%Y%m%d")
    output_dir = os.environ.get("CZDS_OUTPUT", f"./zones/{date_folder}")
    concurrency = int(os.environ.get("CZDS_CONCURRENCY", "3"))

    if not username or not password:
        print("CZDS credentials not set!\n")
        print("Set environment variables:")
        print("  export CZDS_USER='your_email@example.com'")
        print("  export CZDS_PASS='your_password'")
        sys.exit(1)

    print(f"Output directory: {output_dir}")
    print(f"Concurrency: {concurrency}\n")

    async with CZDS(username, password) as client:
        zone_links = await client.fetch_zone_links()
        print(f"Found {len(zone_links)} approved zone files\n")

        if not zone_links:
            print("No approved zone files to download.")
            return

        os.makedirs(output_dir, exist_ok=True)
        zone_links.sort()

        semaphore = asyncio.Semaphore(concurrency)
        tasks = [
            download_zone_compressed(client.session, client.headers, url, output_dir, semaphore)
            for url in zone_links
        ]
        await asyncio.gather(*tasks)

    print(f"\nDone! Zone files saved to: {output_dir}")


if __name__ == "__main__":
    asyncio.run(main())
