import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import time
import pytz
import random
import os
from typing import Dict, Any, List

class AnimeScraperError(Exception):
    """Custom exception for scraper errors"""
    pass

class LivechartScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        }
        self.base_url = "https://www.livechart.me"
        self.output_file = 'output/livechart_episodes.json'
        self.progress_file = 'output/scraping_progress.json'
        self.seasons = ['winter', 'spring', 'summer', 'fall']
        self.years = [2024, 2025, 2026]
        self.content_types = ['tv', 'movies', 'ovas']
        # self.seasons = ['winter']
        # self.years = [2025]
        # self.content_types = ['ovas']

    def load_existing_data(self) -> tuple:
        """Load existing data and progress if available"""
        all_results = []
        progress = {
            'completed': set(),
            'last_position': None
        }
        
        # Load main data file if it exists
        try:
            if os.path.exists(self.output_file):
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    all_results = json.load(f)
                print(f"Loaded {len(all_results)} existing entries from {self.output_file}")
        except Exception as e:
            print(f"Error loading existing data: {str(e)}")

        # Load progress file if it exists
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    temp_progress = json.load(f)
                    progress['completed'] = set(temp_progress['completed'])
                    progress['last_position'] = temp_progress['last_position']
                print(f"Loaded progress information from {self.progress_file}")
        except Exception as e:
            print(f"Error loading progress: {str(e)}")

        return all_results, progress

    def save_progress(self, completed_urls: set, current_position: dict):
        """Save progress information"""
        try:
            progress_data = {
                'completed': list(completed_urls),  # Convert set to list for JSON
                'last_position': current_position
            }
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Error saving progress: {str(e)}")

    def save_results(self, results: list):
        """Save current results to file"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"Saved {len(results)} entries to {self.output_file}")
        except Exception as e:
            print(f"Error saving results: {str(e)}")

    def get_season_key(self, season: str, year: int, content_type: str) -> str:
        """Generate unique key for a season"""
        return f"{season}-{year}-{content_type}"

    def fetch_page_content(self, url: str, max_retries: int = 3, base_delay: float = 3.0) -> str:
        """
        Fetch HTML content from given URL with retry logic and exponential backoff
        
        Args:
            url: The URL to fetch
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between attempts in seconds
        """
        for attempt in range(max_retries):
            try:
                # Add randomized delay between requests
                if attempt > 0:
                    delay = base_delay * (2 ** attempt) + random.uniform(0.1, 1.0)
                    print(f"Waiting {delay:.2f} seconds before retry {attempt + 1}")
                    time.sleep(delay)
                
                response = requests.get(url, headers=self.headers)
                
                # If we get rate limited, wait and retry
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', base_delay))
                    print(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response.text
                
            except requests.RequestException as e:
                if attempt == max_retries - 1:  # Last attempt
                    raise AnimeScraperError(f"Failed to fetch page {url}: {str(e)}")
                print(f"Attempt {attempt + 1} failed: {str(e)}")
                continue

    def convert_timestamp_to_utc(self, timestamp: str) -> str:
        """Convert Unix timestamp to UTC datetime format"""
        try:
            return datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S UTC')
        except (ValueError, TypeError):
            return None

    def extract_page_content(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract detailed information from anime page"""
        try:
            titles = self.extract_titles(soup)
            premiere_season = self.extract_premiere_and_season(soup)
            
            page_content = {
                'jp_title': titles['jp_title'],
                'title': titles['title'],
                'airing_time': self.extract_airing_time(soup),
                'episodes': self.extract_episodes(soup),
                'episode_number': self.extract_episode_number(soup),
                'run_time': self.extract_run_time(soup),
                'status': self.extract_status(soup),
                'premiere': premiere_season['premiere'],
                'season': premiere_season['season'],
                'format': self.extract_format(soup),
            }
            
            # # Remove None values
            # page_content = {k: v for k, v in page_content.items() if v is not None}
            
            return page_content
            
        except Exception as e:
            print(f"Error extracting page content: {str(e)}")
            return {}

    def extract_titles(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Extract both Japanese and English titles
        
        Logic:
        1. Get main name from schema as jp_title
        2. From alternateName array:
           - If array has entries, first entry is English title
           - If no array or empty, fallback to jp_title
        3. Fallback to HTML parsing if schema fails
        """
        titles = {
            'jp_title': None,
            'title': None
        }
        
        try:
            script_tag = soup.find('script', {'type': 'application/ld+json'})
            if script_tag and script_tag.string:
                data = json.loads(script_tag.string)
                
                # Get Japanese title from main name
                titles['jp_title'] = data.get('name')
                
                # Handle alternate names array
                alternate_names = data.get('alternateName', [])
                if isinstance(alternate_names, list) and len(alternate_names) > 1:
                    titles['title'] = alternate_names[0]  # First entry is English title
                else:
                    titles['title'] = titles['jp_title']  # Fallback to Japanese title

            # Fallback to HTML parsing if schema data isn't available or incomplete
            if not titles['jp_title'] or not titles['title']:
                title_divs = soup.find_all('div', {'class': 'text-xl'})
                for div in title_divs:
                    if div.find('span', {'class': 'text-base-content'}):
                        titles['title'] = div.find('span', {'class': 'text-base-content'}).text.strip()
                    elif 'text-base-content' in div.get('class', []):
                        titles['jp_title'] = div.text.strip()
                
                # If still missing title, use jp_title as fallback
                if not titles['title'] and titles['jp_title']:
                    titles['title'] = titles['jp_title']

        except (json.JSONDecodeError, AttributeError) as e:
            print(f"Error extracting titles: {str(e)}")
            pass
        
        return titles

    def extract_mal_id(self, soup: BeautifulSoup) -> str:
        """Extract MAL ID from MyAnimeList URL"""
        try:
            # Try to find MAL link with different possible classes
            mal_link = soup.find('a', {'class': 'mal-icon'}) or \
                      soup.find('a', {'class': 'lc-btn-myanimelist'})
                      
            if mal_link and 'href' in mal_link.attrs:
                match = re.search(r'/(\d+)(?:/|$)', mal_link['href'])
                if match:
                    return match.group(1)
        except AttributeError:
            pass
        return None

    def extract_airing_time(self, soup: BeautifulSoup) -> str:
        """Extract and convert airing timestamp"""
        try:
            countdown_div = soup.find('div', {'data-controller': 'countdown-bar'})
            if countdown_div and 'data-countdown-bar-timestamp' in countdown_div.attrs:
                timestamp = countdown_div['data-countdown-bar-timestamp']
                return self.convert_timestamp_to_utc(timestamp)
        except AttributeError:
            pass
        return None

    def extract_episodes(self, soup: BeautifulSoup) -> str:
        """Extract total episodes information"""
        try:
            grid_items = soup.find_all('div', {'class': 'whitespace-nowrap'})
            for item in grid_items:
                if 'Episodes' in item.text:
                    episodes_span = item.find_next('div', {'class': 'flex'})
                    if episodes_span:
                        text = episodes_span.text.strip()
                        return text if '/' in text else f"0/{text}"
            
            episode_span = soup.find('span', {'class': 'whitespace-pre'})
            if episode_span and episode_span.text:
                episodes_text = episode_span.text.strip()
                if '/' in episodes_text:
                    return episodes_text.strip()
        except AttributeError:
            pass
        return "0/0"

    def extract_episode_number(self, soup: BeautifulSoup) -> int:
        """Extract current episode number from the details page"""
        try:
            # Look for the episode number in the schedules link text
            schedule_link = soup.find('a', {'class': 'line-clamp-1'})
            if schedule_link:
                episode_span = schedule_link.find('span', {'class': 'font-medium'})
                if episode_span:
                    episode_text = episode_span.text.strip()
                    match = re.search(r'EP(\d+)', episode_text)
                    if match:
                        return int(match.group(1))
                        
            # Backup method: look in the release-schedule-info div
            schedule_info = soup.find('div', {'class': 'release-schedule-info'})
            if schedule_info:
                episode_text = schedule_info.text.strip()
                match = re.search(r'EP(\d+)', episode_text)
                if match:
                    return int(match.group(1))

        except (AttributeError, ValueError) as e:
            print(f"Error extracting episode number: {str(e)}")
        
        return None

    def extract_run_time(self, soup: BeautifulSoup) -> str:
        """Extract runtime information"""
        try:
            # Find the specific div with class that contains 'whitespace-nowrap' and text "Run time"
            runtime_label = soup.find('div', {'class': 'text-xs text-base-content/75 whitespace-nowrap', 'string': 'Run time'})
            
            if runtime_label:
                parent = runtime_label.parent
                if parent:
                    # Get all text from parent and remove "Run time"
                    return parent.get_text(strip=True).replace('Run time', '').strip()
            
            # Alternative method if above fails
            grid_container = soup.find('div', class_='grid grid-flow-col auto-cols-fr w-full text-center mb-8 gap-2')
            if grid_container:
                cells = grid_container.find_all('div', recursive=False)
                for cell in cells:
                    label = cell.find('div', string='Run time')
                    if label:
                        # Get text content excluding the label
                        return cell.get_text(strip=True).replace('Run time', '').strip()

        except AttributeError:
            pass
        return None

    def extract_status(self, soup: BeautifulSoup) -> str:
        """Extract status information"""
        try:
            # Try schema.org data first
            script_tag = soup.find('script', {'type': 'application/ld+json'})
            if script_tag and script_tag.string:
                import json
                data = json.loads(script_tag.string)
                if data.get('url', '').endswith('/11908'):  # Verify we have the right data
                    return 'releasing'  # Status is not in schema, but we know it's releasing
            
            # Fallback to HTML parsing
            status_label = soup.find('div', class_='text-sm', text='Status')
            if status_label:
                status_tag = status_label.find_next_sibling(text=True)
                return status_tag.strip() if status_tag else "Unknown"
        except (json.JSONDecodeError, AttributeError):
            pass
        return "Unknown"

    def extract_premiere_and_season(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract premiere date and season"""
        info = {
            'premiere': None,
            'season': None
        }
        
        try:
            # Find the premiere date
            premiere_link = soup.find('a', {'class': 'link link-hover', 'href': re.compile(r'/schedule\?date=')})
            if premiere_link:
                date_str = premiere_link.text.strip()  # e.g. "Dec 11, 2023"
                
                # Parse the date string into a datetime object
                date_obj = datetime.strptime(date_str, '%b %d, %Y')
                
                # Since the site typically lists dates for Japanese releases,
                # assume JST (UTC+9) and convert to UTC
                jst = pytz.timezone('Asia/Tokyo')
                date_jst = jst.localize(date_obj)
                date_utc = date_jst.astimezone(pytz.UTC)
                
                # Format as ISO 8601 UTC string
                info['premiere'] = date_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            # Find the season
            season_link = soup.find('a', {'class': 'link link-hover', 'href': re.compile(r'/[a-z]+-\d{4}/')})
            if season_link:
                info['season'] = season_link.text.strip()

        except (AttributeError, ValueError):
            pass
        
        return info

    def extract_format(self, soup: BeautifulSoup) -> str:
        """Extract anime format"""
        try:
            # This is the container with all metadata including format
            stats_container = soup.find('div', class_='grid grid-flow-col auto-cols-fr w-full text-center mb-8 gap-2')
            if stats_container:
                # Look through each div in the container
                for div in stats_container.find_all('div', recursive=False):
                    # Find the format label div
                    format_label = div.find('div', text='Format')
                    if format_label:
                        # The format value will be in the next sibling div
                        format_value_div = format_label.find_next_sibling()
                        if format_value_div:
                            return format_value_div.text.strip()
                        # If no next sibling, get parent's text excluding 'Format'
                        return div.get_text(strip=True).replace('Format', '').strip()

        except AttributeError:
            pass
        
        return None

    def extract_anime_urls(self, html_content: str) -> List[Dict[str, str]]:
        """Extract anime URLs and basic info from season page"""
        soup = BeautifulSoup(html_content, 'html.parser')
        anime_articles = soup.find_all('article', class_='anime')
        
        anime_data = []
        for article in anime_articles:
            try:
                livechart_id = article.get('data-anime-id')
                title = article.get('data-romaji')
                if not title:
                    title_elem = article.find('h3', class_='main-title')
                    title = title_elem.text.strip() if title_elem else None

                url_elem = article.find('a', href=True)
                relative_url = url_elem['href'] if url_elem else None
                full_url = f"{self.base_url}{relative_url}" if relative_url else None

                # Extract airing timestamp
                countdown_div = article.find('div', {'data-controller': 'countdown-bar'})
                airing_time = None
                if countdown_div and 'data-countdown-bar-timestamp' in countdown_div.attrs:
                    airing_time = countdown_div['data-countdown-bar-timestamp']

                # Get MAL ID
                mal_id = self.extract_mal_id(article)

                anime_data.append({
                    'title': title,
                    'livechart_url': full_url,
                    'livechart_id': livechart_id,
                    'mal_id': mal_id,
                    'airing_time': self.convert_timestamp_to_utc(airing_time) if airing_time else None
                })

            except Exception as e:
                print(f"Error processing article: {str(e)}")
                continue

        return anime_data

    def scrape_anime_details(self, url: str) -> Dict[str, Any]:
        """Scrape detailed information from individual anime page"""
        try:
            html_content = self.fetch_page_content(url)
            soup = BeautifulSoup(html_content, 'html.parser')
            page_content = self.extract_page_content(soup)
            
            # Extract MAL ID from the details page
            mal_id = self.extract_mal_id(soup)  # This will be more reliable
            
            return {
                'page_content': page_content,
                'mal_id': mal_id
            }
        except Exception as e:
            print(f"Error scraping anime details from {url}: {str(e)}")
            return None

    def scrape_season(self, season_url: str) -> List[Dict[str, Any]]:
        """Main function to scrape entire season"""
        try:
            # First get the season page
            html_content = self.fetch_page_content(season_url)
            anime_list = self.extract_anime_urls(html_content)

            # Now scrape each anime page
            final_results = []
            for anime in anime_list:
                try:
                    if anime['livechart_url']:
                        # Get details from anime page
                        details = self.scrape_anime_details(anime['livechart_url'])
                        
                        # Merge master and details data
                        merged_entry = {
                            'livechart_url': anime['livechart_url'],
                            'livechart_id': anime['livechart_id']
                        }
                        
                        if details and details.get('page_content'):
                            # Add all fields from details page_content
                            merged_entry.update(details['page_content'])
                            
                            # Handle MAL ID prioritization
                            details_mal_id = details.get('mal_id')
                            master_mal_id = anime.get('mal_id')
                            merged_entry['mal_id'] = details_mal_id or master_mal_id or None
                        
                        final_results.append(merged_entry)
                        
                        # Add randomized delay between 1-2 seconds
                        delay = random.uniform(1, 2)
                        print(f"Waiting {delay:.2f} seconds before next request...")
                        time.sleep(delay)
                except Exception as e:
                    print(f"Error processing anime {anime.get('title', 'Unknown')}: {str(e)}")
                    continue

            return final_results

        except Exception as e:
            print(f"Error scraping season: {str(e)}")
            return []

    def merge_anime_data(self, master_data: Dict[str, Any], details_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge data from master page and details page with correct prioritization
        
        Priority rules:
        - titles and airing_time from details page
        - mal_id from either source (non-null)
        - flatten page_content structure
        """
        try:
            merged_data = {
                'livechart_url': master_data.get('livechart_url'),
                'livechart_id': master_data.get('livechart_id'),
                'scrape_info': master_data.get('scrape_info')
            }

            # Get page content from details
            if details_data and 'page_content' in details_data:
                page_content = details_data['page_content'].get('page_content', {})
                
                # Add all page content fields to top level
                for key, value in page_content.items():
                    merged_data[key] = value

            # Handle MAL ID prioritization
            details_mal_id = details_data.get('mal_id') if details_data else None
            master_mal_id = master_data.get('mal_id')
            merged_data['mal_id'] = details_mal_id or master_mal_id or None

            return merged_data
            
        except Exception as e:
            print(f"Error merging data: {str(e)}")
            return master_data  # Return original data if merge fails

def main():
    scraper = LivechartScraper()

    # Load existing data and progress
    all_results, progress = scraper.load_existing_data()
    completed_urls = progress['completed']
    last_position = progress['last_position']
    
    # Determine starting position
    start_found = False if last_position else True
    
    try:
        for year in scraper.years:
            for season in scraper.seasons:
                for content_type in scraper.content_types:
                    current_position = {
                        'year': year,
                        'season': season,
                        'content_type': content_type
                    }
                    
                    # Skip until we find last position if resuming
                    if not start_found:
                        if (current_position['year'] == last_position['year'] and 
                            current_position['season'] == last_position['season'] and 
                            current_position['content_type'] == last_position['content_type']):
                            start_found = True
                        else:
                            continue

                    season_url = f"https://www.livechart.me/{season}-{year}/{content_type}"
                    print(f"\nScraping {season.capitalize()} {year} {content_type}...")
                    
                    try:
                        results = scraper.scrape_season(season_url)
                        
                        # Process results and update completed URLs
                        for entry in results:
                            if entry['livechart_url'] not in completed_urls:
                                entry['scrape_info'] = {
                                    'season': season.capitalize(),
                                    'year': year,
                                    'content_type': content_type
                                }
                                all_results.append(entry)
                                completed_urls.add(entry['livechart_url'])
                        
                        # Save progress after each season
                        scraper.save_progress(completed_urls, current_position)
                        scraper.save_results(all_results)
                        
                        print(f"Successfully scraped {len(results)} entries from {season.capitalize()} {year} {content_type}")
                        print(f"Total entries so far: {len(all_results)}")
                        
                        # Add longer delay between seasons
                        delay = random.uniform(5, 8)
                        print(f"Waiting {delay:.2f} seconds before next season...")
                        time.sleep(delay)
                        
                    except Exception as e:
                        print(f"Error scraping {season} {year}: {str(e)}")
                        # Save progress even on error
                        scraper.save_progress(completed_urls, current_position)
                        scraper.save_results(all_results)
                        continue

    except KeyboardInterrupt:
        print("\nScraping interrupted by user. Saving progress...")
        scraper.save_progress(completed_urls, current_position)
        scraper.save_results(all_results)
        print("Progress saved. You can resume later.")
        return

    # Final save
    scraper.save_progress(completed_urls, current_position)
    scraper.save_results(all_results)
    print(f"\nScraping completed! Total entries: {len(all_results)}")

if __name__ == "__main__":
    main()
