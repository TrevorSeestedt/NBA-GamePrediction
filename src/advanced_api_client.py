"""
Advanced NBA API Client
Direct scraping from NBA.com to get chemistry stats and advanced data
not available in the nba-api library
"""

import requests
import pandas as pd
import time
import logging
from bs4 import BeautifulSoup
from datetime import datetime
import json
import re

logger = logging.getLogger(__name__)

class NBADirectAPIClient:
    """
    Direct API client for NBA.com endpoints that aren't available in nba-api
    
    This client scrapes the exact pages you identified to get chemistry stats
    and other advanced metrics for our prediction system.
    """
    
    def __init__(self, rate_limit_delay=2.0):
        """
        Initialize the direct API client
        
        Args:
            rate_limit_delay (float): Seconds to wait between requests
        """
        self.rate_limit_delay = rate_limit_delay
        self.base_url = "https://www.nba.com/stats"
        
        # Headers to mimic a real browser and avoid blocking
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Host': 'stats.nba.com',
            'Pragma': 'no-cache',
            'Referer': 'https://www.nba.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'x-nba-stats-origin': 'stats',
            'x-nba-stats-token': 'true'
        }
        
        logger.info("🔗 Advanced NBA API Client initialized")
        logger.info(f"⏱️ Rate limit: {rate_limit_delay}s between requests")
    
    def _rate_limit(self):
        """Apply rate limiting between requests"""
        time.sleep(self.rate_limit_delay)
    
    def _make_request(self, url, params=None):
        """
        Make a request to NBA.com with proper error handling
        
        Args:
            url (str): The URL to request
            params (dict): Query parameters
            
        Returns:
            requests.Response or None: Response object or None if failed
        """
        try:
            self._rate_limit()
            response = requests.get(url, headers=self.headers, params=params, timeout=15)
            
            if response.status_code == 200:
                return response
            else:
                logger.error(f"❌ HTTP {response.status_code} for {url}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"❌ Request failed for {url}: {e}")
            return None
    
    def collect_hustle_stats(self, season='2024-25', season_type='Regular Season'):
        """
        🎯 CHEMISTRY STATS: Collect team hustle statistics
        
        This gets the EXACT chemistry metrics you designed:
        - Screen Assists (offensive chemistry)
        - Deflections (defensive chemistry) 
        - Contested Shots (defensive chemistry)
        
        Args:
            season (str): NBA season (e.g., '2024-25')
            season_type (str): 'Regular Season' or 'Playoffs'
            
        Returns:
            pd.DataFrame: Hustle stats with chemistry metrics
        """
        logger.info(f"🏃‍♂️ Collecting hustle stats for {season} {season_type}")
        
        # Build URL based on season type
        season_param = 'Regular+Season' if season_type == 'Regular Season' else 'Playoffs'
        url = f"https://www.nba.com/stats/teams/hustle?SeasonType={season_param}"
        
        logger.info(f"📡 Requesting: {url}")
        
        response = self._make_request(url)
        if not response:
            return pd.DataFrame()
        
        try:
            # The NBA.com pages often load data via JavaScript/AJAX
            # We need to extract the actual API endpoint they call
            
            # Look for the stats API call in the page source
            page_content = response.text
            
            # Find the API endpoint pattern
            api_pattern = r'stats\.nba\.com/stats/([^"\']+)'
            matches = re.findall(api_pattern, page_content)
            
            if matches:
                # Try the API endpoint directly
                api_endpoint = matches[0]
                api_url = f"https://stats.nba.com/stats/{api_endpoint}"
                
                api_response = self._make_request(api_url)
                if api_response:
                    data = api_response.json()
                    
                    if 'resultSets' in data and len(data['resultSets']) > 0:
                        result_set = data['resultSets'][0]
                        headers_list = result_set['headers']
                        rows = result_set['rowSet']
                        
                        df = pd.DataFrame(rows, columns=headers_list)
                        
                        # Add metadata
                        df['season'] = season
                        df['season_type'] = season_type
                        df['collected_at'] = datetime.now()
                        
                        logger.info(f"✅ Collected hustle stats: {len(df)} teams")
                        logger.info(f"🎯 Columns: {list(df.columns)}")
                        
                        # Look for our chemistry metrics
                        chemistry_cols = [col for col in df.columns 
                                        if any(keyword in col.upper() 
                                              for keyword in ['SCREEN', 'DEFLECT', 'CONTEST'])]
                        
                        if chemistry_cols:
                            logger.info(f"🧪 Chemistry metrics found: {chemistry_cols}")
                        
                        return df
            
            # Fallback: Try to parse HTML table
            logger.info("🔄 Falling back to HTML table parsing...")
            return self._parse_html_table(response.text, 'hustle')
            
        except Exception as e:
            logger.error(f"❌ Error processing hustle stats: {e}")
            return pd.DataFrame()
    
    def collect_box_outs(self, season='2024-25', season_type='Regular Season'):
        """
        📦 CHEMISTRY STATS: Collect team box-out statistics
        
        Box-outs show team coordination and effort on rebounds
        
        Args:
            season (str): NBA season
            season_type (str): Season type
            
        Returns:
            pd.DataFrame: Box-out stats
        """
        logger.info(f"📦 Collecting box-out stats for {season} {season_type}")
        
        season_param = 'Regular+Season' if season_type == 'Regular Season' else 'Playoffs'
        url = f"https://www.nba.com/stats/teams/box-outs?SeasonType={season_param}"
        
        response = self._make_request(url)
        if not response:
            return pd.DataFrame()
        
        try:
            # Similar process as hustle stats
            return self._extract_stats_data(response, 'box-outs', season, season_type)
        except Exception as e:
            logger.error(f"❌ Error collecting box-outs: {e}")
            return pd.DataFrame()
    
    def collect_team_defense(self, season='2024-25', season_type='Regular Season'):
        """
        🛡️ Collect team defensive dashboard stats
        
        Args:
            season (str): NBA season
            season_type (str): Season type
            
        Returns:
            pd.DataFrame: Defensive stats
        """
        logger.info(f"🛡️ Collecting team defense for {season} {season_type}")
        
        season_param = 'Regular+Season' if season_type == 'Regular Season' else 'Playoffs'
        url = f"https://www.nba.com/stats/teams/defense-dash-overall?SeasonType={season_param}"
        
        response = self._make_request(url)
        if not response:
            return pd.DataFrame()
        
        try:
            return self._extract_stats_data(response, 'defense', season, season_type)
        except Exception as e:
            logger.error(f"❌ Error collecting team defense: {e}")
            return pd.DataFrame()
    
    def collect_opponent_shooting(self, season='2024-25', season_type='Regular Season'):
        """
        🎯 Collect team opponent shooting by distance
        
        Args:
            season (str): NBA season
            season_type (str): Season type
            
        Returns:
            pd.DataFrame: Opponent shooting stats by distance
        """
        logger.info(f"🎯 Collecting opponent shooting for {season} {season_type}")
        
        season_param = 'Regular+Season' if season_type == 'Regular Season' else 'Playoffs'
        url = f"https://www.nba.com/stats/teams/opponent-shooting?SeasonType={season_param}"
        
        response = self._make_request(url)
        if not response:
            return pd.DataFrame()
        
        try:
            return self._extract_stats_data(response, 'opponent-shooting', season, season_type)
        except Exception as e:
            logger.error(f"❌ Error collecting opponent shooting: {e}")
            return pd.DataFrame()
    
    def collect_transition_stats(self, season='2024-25', season_type='Regular Season'):
        """
        🏃‍♂️ Collect team transition statistics
        
        Args:
            season (str): NBA season
            season_type (str): Season type
            
        Returns:
            pd.DataFrame: Transition stats
        """
        logger.info(f"🏃‍♂️ Collecting transition stats for {season} {season_type}")
        
        season_param = 'Regular+Season' if season_type == 'Regular Season' else 'Playoffs'
        url = f"https://www.nba.com/stats/teams/transition?SeasonType={season_param}"
        
        response = self._make_request(url)
        if not response:
            return pd.DataFrame()
        
        try:
            return self._extract_stats_data(response, 'transition', season, season_type)
        except Exception as e:
            logger.error(f"❌ Error collecting transition stats: {e}")
            return pd.DataFrame()
    
    def _extract_stats_data(self, response, stat_type, season, season_type):
        """
        Helper method to extract stats data from NBA.com response
        
        Args:
            response: HTTP response object
            stat_type (str): Type of stats being collected
            season (str): NBA season
            season_type (str): Season type
            
        Returns:
            pd.DataFrame: Extracted stats data
        """
        try:
            # Try to find API endpoint in page source
            page_content = response.text
            api_pattern = r'stats\.nba\.com/stats/([^"\']+)'
            matches = re.findall(api_pattern, page_content)
            
            if matches:
                # Try direct API call
                api_endpoint = matches[0]
                api_url = f"https://stats.nba.com/stats/{api_endpoint}"
                
                api_response = self._make_request(api_url)
                if api_response:
                    data = api_response.json()
                    
                    if 'resultSets' in data and len(data['resultSets']) > 0:
                        result_set = data['resultSets'][0]
                        headers_list = result_set['headers']
                        rows = result_set['rowSet']
                        
                        df = pd.DataFrame(rows, columns=headers_list)
                        
                        # Add metadata
                        df['season'] = season
                        df['season_type'] = season_type
                        df['stat_type'] = stat_type
                        df['collected_at'] = datetime.now()
                        
                        logger.info(f"✅ Collected {stat_type}: {len(df)} records")
                        return df
            
            # Fallback to HTML parsing
            return self._parse_html_table(response.text, stat_type)
            
        except Exception as e:
            logger.error(f"❌ Error extracting {stat_type} data: {e}")
            return pd.DataFrame()
    
    def _parse_html_table(self, html_content, stat_type):
        """
        Fallback method to parse HTML tables when API extraction fails
        
        Args:
            html_content (str): HTML content
            stat_type (str): Type of stats
            
        Returns:
            pd.DataFrame: Parsed table data
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for data tables
            tables = soup.find_all('table')
            
            if not tables:
                logger.warning(f"⚠️ No tables found for {stat_type}")
                return pd.DataFrame()
            
            # Try to find the main stats table
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) > 1:  # Has header and data rows
                    
                    # Extract headers
                    header_row = rows[0]
                    headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
                    
                    # Extract data rows
                    data_rows = []
                    for row in rows[1:]:
                        cells = row.find_all(['td', 'th'])
                        row_data = [cell.get_text().strip() for cell in cells]
                        if len(row_data) == len(headers):
                            data_rows.append(row_data)
                    
                    if data_rows:
                        df = pd.DataFrame(data_rows, columns=headers)
                        logger.info(f"✅ Parsed HTML table for {stat_type}: {len(df)} rows")
                        return df
            
            logger.warning(f"⚠️ Could not parse table for {stat_type}")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"❌ HTML parsing failed for {stat_type}: {e}")
            return pd.DataFrame()
    
    def collect_all_chemistry_stats(self, season='2024-25'):
        """
        🧪 Collect ALL chemistry-related stats for both regular season and playoffs
        
        This is the main method that gets all the data you identified for
        the Chemistry Index calculation.
        
        Args:
            season (str): NBA season
            
        Returns:
            dict: Dictionary containing all collected stats
        """
        logger.info(f"🧪 Starting comprehensive chemistry stats collection for {season}")
        
        results = {}
        
        # Define all the stats we want to collect
        stat_methods = {
            'hustle_regular': lambda: self.collect_hustle_stats(season, 'Regular Season'),
            'hustle_playoffs': lambda: self.collect_hustle_stats(season, 'Playoffs'),
            'box_outs_regular': lambda: self.collect_box_outs(season, 'Regular Season'),
            'box_outs_playoffs': lambda: self.collect_box_outs(season, 'Playoffs'),
            'defense_regular': lambda: self.collect_team_defense(season, 'Regular Season'),
            'defense_playoffs': lambda: self.collect_team_defense(season, 'Playoffs'),
            'opponent_shooting_regular': lambda: self.collect_opponent_shooting(season, 'Regular Season'),
            'opponent_shooting_playoffs': lambda: self.collect_opponent_shooting(season, 'Playoffs'),
            'transition_regular': lambda: self.collect_transition_stats(season, 'Regular Season'),
            'transition_playoffs': lambda: self.collect_transition_stats(season, 'Playoffs'),
        }
        
        # Collect each stat type
        for stat_name, method in stat_methods.items():
            try:
                logger.info(f"📊 Collecting {stat_name}...")
                data = method()
                results[stat_name] = data
                
                if not data.empty:
                    logger.info(f"✅ {stat_name}: {len(data)} records collected")
                else:
                    logger.warning(f"⚠️ {stat_name}: No data collected")
                    
            except Exception as e:
                logger.error(f"❌ Failed to collect {stat_name}: {e}")
                results[stat_name] = pd.DataFrame()
        
        # Summary
        total_records = sum(len(df) for df in results.values() if not df.empty)
        successful_collections = sum(1 for df in results.values() if not df.empty)
        
        logger.info(f"🎉 Chemistry stats collection complete!")
        logger.info(f"📊 Successful collections: {successful_collections}/{len(stat_methods)}")
        logger.info(f"📈 Total records: {total_records}")
        
        return results

# Example usage
if __name__ == "__main__":
    print("🧪 NBA Advanced API Client - Test Run")
    print("=" * 50)
    
    # Initialize client
    client = NBADirectAPIClient()
    
    # Test hustle stats collection (this has our chemistry metrics!)
    print("\n🏃‍♂️ Testing hustle stats collection...")
    hustle_data = client.collect_hustle_stats('2024-25', 'Regular Season')
    
    if not hustle_data.empty:
        print(f"✅ Success! Collected {len(hustle_data)} teams")
        print(f"🎯 Columns: {list(hustle_data.columns)}")
        
        # Look for chemistry metrics
        chemistry_cols = [col for col in hustle_data.columns 
                         if any(keyword in col.upper() 
                               for keyword in ['SCREEN', 'DEFLECT', 'CONTEST'])]
        
        if chemistry_cols:
            print(f"🧪 CHEMISTRY METRICS FOUND: {chemistry_cols}")
        else:
            print("⚠️ Chemistry metrics not found in expected format")
    else:
        print("❌ Failed to collect hustle stats")
    
    print("\n🧪 Advanced API Client test complete!") 