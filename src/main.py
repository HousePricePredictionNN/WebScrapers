import random
import time

import pandas as pd
from tqdm import tqdm

from headers import Headers
from scrapers.olxscraper import OlxScraper, scrape_offer_details as get_olx_offer
from scrapers.otodomscraper import OtodomScraper, scrape_offer_details as get_otodom_offer
import os
import threading
import concurrent.futures

def get_offer_details_threaded(links, max_workers=10):
    # Initialize dictionary for scraped data
    scraped_data = {key: [] for key in [list(Headers)[i].value for i in range(len(Headers))]}
    
    # Create a default/empty data template with all required keys
    empty_data = {key: '' for key in [list(Headers)[i].value for i in range(len(Headers))]}

    # Thread-safe lock for appending results
    lock = threading.Lock()

    # Counter for progress reporting
    counter = {'processed': 0, 'total': len(links)}

    def process_link(link):
        try:
            # Random delay between requests (to avoid being blocked)
            time.sleep(random.uniform(1, 2))

            # Use appropriate scraper based on the domain
            if 'otodom' in link:
                data = get_otodom_offer(link)
            else:
                data = get_olx_offer(link)
            
            # Ensure all required keys exist
            result_data = {}
            for key in scraped_data.keys():
                result_data[key] = data.get(key, '')
                    
            with lock:
                counter['processed'] += 1
                if counter['processed'] % 100 == 0:
                    print(f"Processed {counter['processed']} out of {counter['total']} links")
                return result_data      
                
        except Exception as e:
            # Log the error but continue with next link
            print(f"Error processing {link}: {str(e)}")
            with lock:
                counter['processed'] += 1
                return None

    # Process links using ThreadPoolExecutor
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_link = {executor.submit(process_link, link): link for link in links}
        
        # Use tqdm to show progress
        for future in tqdm(concurrent.futures.as_completed(future_to_link), total=len(links), desc="Scraping offer details"):
            link = future_to_link[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as exc:
                print(f'{link} generated an exception: {exc}')
                results.append(empty_data)
    
    results = [result for result in results if result is not None]

    # Convert results to required format
    for key in scraped_data.keys():
        scraped_data[key] = [result.get(key, '') for result in results]
    
    return scraped_data


def remove_duplicates(data):
    # Create a dictionary to track unique links
    seen_links = {}
    indices_to_remove = []
    
    for i, link in enumerate(data[Headers.LINK.value]):
        # Skip empty links or skip entire records with empty links
        if not link or link.strip() == '':
            continue
            
        # If we've seen this link before, mark it for removal
        if link in seen_links:
            indices_to_remove.append(i)
        else:
            seen_links[link] = i
    
    # Remove duplicates (from highest index to lowest to avoid shifting problems)
    for index in sorted(indices_to_remove, reverse=True):
        for key in data.keys():
            data[key].pop(index)
    
    return data

def process_data_source(scraper, source_name, pages, resources_dir, max_workers=10, resume=False):
    """Process a single data source (OLX or Otodom) with checkpoint saving"""
    # Define all output files
    data_file = os.path.join(resources_dir, f'{source_name}_data.csv')
    checkpoint_file = os.path.join(resources_dir, f'{source_name}_checkpoint.csv')
    progress_file = os.path.join(resources_dir, f'{source_name}_progress.txt')
    
    # Check if we should resume from a previous run
    last_completed_batch = -1
    if resume and os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                last_completed_batch = int(f.read().strip())
            print(f"Resuming {source_name} scraping from batch {last_completed_batch + 1}")
        except:
            print(f"Could not read progress file for {source_name}, starting from beginning")
    
    # Get initial data only if not resuming or if checkpoint doesn't exist
    if last_completed_batch == -1:
        print(f"Scraping {source_name} offers...")
        data = scraper.scrape_offers(pages=pages)
        
        # Add source information to each record
        data[Headers.SOURCE.value] = [source_name] * len(data[Headers.LINK.value])
        
        # Save initial data as checkpoint
        pd.DataFrame(data).to_csv(checkpoint_file, index=False, encoding='utf-8')
        
        # Reset progress tracking
        with open(progress_file, 'w') as f:
            f.write('-1')  # No batches completed yet
    else:
        # Load from checkpoint
        print(f"Loading {source_name} data from checkpoint...")
        checkpoint_df = pd.read_csv(checkpoint_file)
        data = {column: checkpoint_df[column].tolist() for column in checkpoint_df.columns}
    
    # Process in smaller batches and save progress
    batch_size = 100
    all_links = data[Headers.LINK.value]
    total_batches = len(all_links) // batch_size + (1 if len(all_links) % batch_size > 0 else 0)
    
    for batch_num in range(total_batches):
        # Skip already processed batches if resuming
        if batch_num <= last_completed_batch:
            continue
            
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(all_links))
        batch_links = all_links[start_idx:end_idx]
        
        try:
            print(f"Processing {source_name} batch {batch_num+1}/{total_batches} (links {start_idx+1}-{end_idx})")
            batch_data = get_offer_details_threaded(batch_links, max_workers)
            
            # Add source information to batch data
            batch_data[Headers.SOURCE.value] = [source_name] * len(batch_data[Headers.LINK.value])
            
            # Update data with batch results
            for key in data.keys():
                if start_idx < len(data[key]):
                    # Handle case where data might have fewer entries than expected
                    end_actual = min(end_idx, len(data[key]))
                    # Make sure we don't exceed the length of batch_data[key]
                    num_entries = min(end_actual - start_idx, len(batch_data[key]))
                    data[key][start_idx:start_idx + num_entries] = batch_data[key][:num_entries]
            
            # Update the main checkpoint after each batch
            pd.DataFrame(data).to_csv(checkpoint_file, index=False, encoding='utf-8')
            
            # Update progress tracker
            with open(progress_file, 'w') as f:
                f.write(str(batch_num))
                
            print(f"{source_name} batch {batch_num+1} complete and saved")
            
        except Exception as e:
            print(f"Error processing batch {batch_num+1}: {str(e)}")
            print(f"Last successfully processed batch: {batch_num}")
            # Don't update progress file - this will allow resuming from this failed batch
            import traceback
            print(traceback.format_exc())
            break
    
    # Remove duplicate entries
    data = remove_duplicates(data)
    
    # Save final results
    dataframe = pd.DataFrame(data)
    dataframe.to_csv(data_file, index=False, encoding='utf-8')
    
    # Clean up progress files after successful completion
    try:
        os.remove(progress_file)
    except:
        pass
        
    print(f"Completed processing {source_name} data, saved to {data_file}")
    return data

def main():
    # Scraper settings
    olx_scraper = OlxScraper()
    otodom_scraper = OtodomScraper()

    # Data file paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    resources_dir = os.path.join(base_dir, 'resources')
    
    # Create resources directory if it doesn't exist
    os.makedirs(resources_dir, exist_ok=True)
    
    # Check for resume flag
    resume = True  # Set to True to enable auto-resuming from checkpoints
    
    # Process each data source separately
    data_olx = process_data_source(olx_scraper, 'olx', pages=24, resources_dir=resources_dir, resume=resume)
    data_otodom = process_data_source(otodom_scraper, 'otodom', pages=250, resources_dir=resources_dir, resume=resume)

    # Combine data from both scrapers and save combined file
    combined_file = os.path.join(resources_dir, 'combined_data.csv')
    
    # Use pandas to combine and save
    df_olx = pd.DataFrame(data_olx)
    df_otodom = pd.DataFrame(data_otodom)
    combined_df = pd.concat([df_olx, df_otodom], ignore_index=True)
    
    # Remove duplicates from combined dataset
    combined_df.drop_duplicates(subset=[Headers.LINK.value], keep='first', inplace=True)
    
    # Save combined data
    combined_df.to_csv(combined_file, index=False, encoding='utf-8')
    print(f"Combined data saved to {combined_file}")

if __name__ == '__main__':
    main()