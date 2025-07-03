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

    def get_clutch_stats(self, season_type='Regular Season'):
        """
        Get clutch performance stats for all teams
        
        Clutch stats show how teams perform in close games (within 5 points in last 5 minutes).
        This is crucial for predictions as it reveals which teams excel under pressure.
        
        Args:
            season_type (str): 'Regular Season' or 'Playoffs'
            
        Returns:
            list: Team clutch statistics
        """
        logger.info(f"🔥 Collecting clutch stats for {season_type}")
        
        # Map season type to URL parameter
        season_param = season_type.replace(' ', '+')
        url = f"https://www.nba.com/stats/teams/clutch-traditional?SeasonType={season_param}"
        
        logger.info(f"📡 Fetching from: {url}")
        
        try:
            # Make request with proper headers 
            response = self.session.get(
                url, 
                headers=self.headers, 
                timeout=15,
                allow_redirects=True
            )
            
            if response.status_code != 200:
                logger.error(f"❌ HTTP {response.status_code} for clutch stats")
                return []
            
            # Try to extract JSON data from the page
            clutch_data = self._extract_stats_data(response.text, 'clutch')
            
            if clutch_data:
                logger.info(f"✅ Found {len(clutch_data)} teams with clutch data")
                return self._process_clutch_data(clutch_data, season_type)
            
            # Fallback to HTML parsing
            logger.info("🔄 Falling back to HTML parsing for clutch stats")
            return self._parse_clutch_html(response.text, season_type)
            
        except Exception as e:
            logger.error(f"❌ Error collecting clutch stats: {e}")
            return []
    
    def _process_clutch_data(self, raw_data, season_type):
        """
        Process raw clutch data into structured format
        
        Expected columns: Team, GP, W, L, WIN%, Min, PTS, FGM, FGA, FG%, 
                         3PM, 3PA, 3P%, FTM, FTA, FT%, OREB, DREB, REB, 
                         AST, TOV, STL, BLK, BLKA, PF, PFD, +/-
        """
        logger.info("🔧 Processing clutch data...")
        
        processed_stats = []
        
        try:
            for team_data in raw_data:
                clutch_record = {
                    'team_name': team_data.get('TEAM_NAME', ''),
                    'team_id': int(team_data.get('TEAM_ID', 0)),
                    'season_type': season_type,
                    
                    # Game record in clutch situations
                    'clutch_games': int(team_data.get('GP', 0)),
                    'clutch_wins': int(team_data.get('W', 0)),
                    'clutch_losses': int(team_data.get('L', 0)),
                    'clutch_win_pct': float(team_data.get('WIN_PCT', 0)),
                    
                    # Time spent in clutch situations
                    'clutch_minutes': float(team_data.get('MIN', 0)),
                    
                    # Offensive clutch performance
                    'clutch_points': float(team_data.get('PTS', 0)),
                    'clutch_fgm': float(team_data.get('FGM', 0)),
                    'clutch_fga': float(team_data.get('FGA', 0)),
                    'clutch_fg_pct': float(team_data.get('FG_PCT', 0)),
                    'clutch_3pm': float(team_data.get('FG3M', 0)),
                    'clutch_3pa': float(team_data.get('FG3A', 0)),
                    'clutch_3p_pct': float(team_data.get('FG3_PCT', 0)),
                    'clutch_ftm': float(team_data.get('FTM', 0)),
                    'clutch_fta': float(team_data.get('FTA', 0)),
                    'clutch_ft_pct': float(team_data.get('FT_PCT', 0)),
                    
                    # Rebounding in clutch
                    'clutch_oreb': float(team_data.get('OREB', 0)),
                    'clutch_dreb': float(team_data.get('DREB', 0)),
                    'clutch_reb': float(team_data.get('REB', 0)),
                    
                    # Playmaking and defense in clutch
                    'clutch_ast': float(team_data.get('AST', 0)),
                    'clutch_tov': float(team_data.get('TOV', 0)),
                    'clutch_stl': float(team_data.get('STL', 0)),
                    'clutch_blk': float(team_data.get('BLK', 0)),
                    'clutch_blka': float(team_data.get('BLKA', 0)),  # Blocks against
                    'clutch_pf': float(team_data.get('PF', 0)),
                    'clutch_pfd': float(team_data.get('PFD', 0)),   # Personal fouls drawn
                    'clutch_plus_minus': float(team_data.get('PLUS_MINUS', 0)),
                    
                    # Calculated clutch efficiency metrics
                    'clutch_offensive_rating': 0,  # Will calculate if we have possessions
                    'clutch_defensive_rating': 0,
                    'clutch_ast_to_ratio': 0,
                    'clutch_ts_pct': 0,  # True shooting percentage
                    
                    'collected_at': datetime.now()
                }
                
                # Calculate additional clutch metrics
                clutch_record = self._calculate_clutch_metrics(clutch_record)
                processed_stats.append(clutch_record)
                
                logger.info(f"✅ {clutch_record['team_name']}: "
                          f"{clutch_record['clutch_wins']}-{clutch_record['clutch_losses']} "
                          f"({clutch_record['clutch_win_pct']:.1%}) in clutch")
                
        except Exception as e:
            logger.error(f"❌ Error processing clutch data: {e}")
        
        return processed_stats
    
    def _calculate_clutch_metrics(self, clutch_record):
        """Calculate additional clutch performance metrics"""
        try:
            # Assist-to-turnover ratio in clutch
            if clutch_record['clutch_tov'] > 0:
                clutch_record['clutch_ast_to_ratio'] = clutch_record['clutch_ast'] / clutch_record['clutch_tov']
            
            # True shooting percentage in clutch
            # TS% = PTS / (2 * (FGA + 0.44 * FTA))
            if clutch_record['clutch_fga'] > 0 or clutch_record['clutch_fta'] > 0:
                ts_denominator = 2 * (clutch_record['clutch_fga'] + 0.44 * clutch_record['clutch_fta'])
                if ts_denominator > 0:
                    clutch_record['clutch_ts_pct'] = clutch_record['clutch_points'] / ts_denominator
            
            # Clutch scoring efficiency (points per field goal attempt)
            if clutch_record['clutch_fga'] > 0:
                clutch_record['clutch_scoring_efficiency'] = clutch_record['clutch_points'] / clutch_record['clutch_fga']
            else:
                clutch_record['clutch_scoring_efficiency'] = 0
                
        except Exception as e:
            logger.error(f"❌ Error calculating clutch metrics: {e}")
        
        return clutch_record
    
    def _parse_clutch_html(self, html_content, season_type):
        """Fallback HTML parsing for clutch stats"""
        logger.info("🔍 Parsing clutch HTML content...")
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for tables with clutch data
            tables = soup.find_all('table')
            
            for table in tables:
                # Try to identify clutch stats table
                headers = table.find_all(['th', 'td'])
                header_text = [h.get_text().strip().lower() for h in headers[:10]]
                
                # Check if this looks like clutch stats
                if any(keyword in ' '.join(header_text) for keyword in ['win%', 'clutch', 'pts', 'fg%']):
                    logger.info("🎯 Found potential clutch stats table")
                    return self._extract_clutch_from_table(table, season_type)
            
            logger.warning("⚠️ No clutch stats table found in HTML")
            return []
            
        except Exception as e:
            logger.error(f"❌ Error parsing clutch HTML: {e}")
            return []
    
    def _extract_clutch_from_table(self, table, season_type):
        """Extract clutch data from HTML table"""
        clutch_stats = []
        
        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return clutch_stats
            
            # Get headers
            header_row = rows[0]
            headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
            
            # Process data rows
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 5:  # Need minimum columns
                    continue
                
                row_data = [cell.get_text().strip() for cell in cells]
                
                # Create clutch record (simplified version)
                if len(row_data) >= 5:
                    clutch_record = {
                        'team_name': row_data[0],
                        'season_type': season_type,
                        'clutch_games': self._safe_int(row_data[1]) if len(row_data) > 1 else 0,
                        'clutch_wins': self._safe_int(row_data[2]) if len(row_data) > 2 else 0,
                        'clutch_losses': self._safe_int(row_data[3]) if len(row_data) > 3 else 0,
                        'clutch_win_pct': self._safe_float(row_data[4]) if len(row_data) > 4 else 0,
                        'collected_at': datetime.now()
                    }
                    
                    clutch_stats.append(clutch_record)
            
            logger.info(f"📊 Extracted {len(clutch_stats)} clutch records from HTML")
            
        except Exception as e:
            logger.error(f"❌ Error extracting clutch table data: {e}")
        
        return clutch_stats

    def get_positional_defense_stats(self):
        """
        Get positional defense statistics from Hashtag Basketball
        
        This data shows how each team defends against specific positions (PG, SG, SF, PF, C).
        Crucial for matchup analysis - if a team is weak defending PGs and opponent has a great PG,
        that's a significant advantage.
        
        Returns:
            list: Positional defense statistics for all teams and positions
        """
        logger.info("🛡️ Collecting positional defense stats from Hashtag Basketball")
        
        url = "https://hashtagbasketball.com/nba-defense-vs-position"
        logger.info(f"📡 Fetching from: {url}")
        
        try:
            # Make request with proper headers
            response = self.session.get(url, headers=self.headers, timeout=15)
            
            if response.status_code != 200:
                logger.error(f"❌ HTTP {response.status_code} for positional defense")
                return []
            
            logger.info(f"✅ Got response ({len(response.text)} chars)")
            
            # Parse the HTML to extract positional defense data
            return self._parse_positional_defense_html(response.text)
            
        except Exception as e:
            logger.error(f"❌ Error collecting positional defense stats: {e}")
            return []
    
    def _parse_positional_defense_html(self, html_content):
        """
        Parse Hashtag Basketball positional defense data from HTML
        
        Expected format: Position | Team | PTS | FG% | FT% | 3PM | REB | AST | STL | BLK | TO
        """
        logger.info("🔍 Parsing positional defense HTML...")
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the main data table (usually the largest table with position data)
            tables = soup.find_all('table')
            
            positional_data = []
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 10:  # Skip small tables
                    continue
                
                # Look for header row with expected columns
                header_row = rows[0] if rows else None
                if not header_row:
                    continue
                
                headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
                
                # Check if this looks like the positional defense table
                expected_cols = ['position', 'team', 'pts', 'fg%', 'ft%', '3pm', 'reb', 'ast', 'stl', 'blk', 'to']
                header_lower = [h.lower() for h in headers]
                
                if not any(col in ' '.join(header_lower) for col in ['position', 'team', 'pts']):
                    continue
                
                logger.info(f"📊 Found positional defense table with {len(rows)} rows")
                logger.info(f"Headers: {headers[:10]}...")
                
                # Process data rows
                for row in rows[1:]:  # Skip header
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 5:  # Need minimum columns
                        continue
                    
                    row_data = [cell.get_text().strip() for cell in cells]
                    
                    # Extract position and team info
                    if len(row_data) >= 10:
                        try:
                            position = row_data[0]
                            team_info = row_data[1]
                            
                            # Extract team abbreviation (usually first 3 chars or before space/number)
                            team_abbrev = team_info.split()[0][:3] if team_info else ''
                            
                            # Skip if no valid position or team
                            if not position or not team_abbrev or len(position) > 2:
                                continue
                            
                            # Create positional defense record
                            defense_record = {
                                'position': position.upper(),
                                'team_abbrev': team_abbrev.upper(),
                                'team_info': team_info,
                                
                                # Defensive stats allowed to this position
                                'pts_allowed': self._safe_float(row_data[2]),
                                'fg_pct_allowed': self._safe_float(row_data[3]),
                                'ft_pct_allowed': self._safe_float(row_data[4]),
                                'threes_allowed': self._safe_float(row_data[5]),
                                'reb_allowed': self._safe_float(row_data[6]),
                                'ast_allowed': self._safe_float(row_data[7]),
                                'stl_allowed': self._safe_float(row_data[8]),
                                'blk_allowed': self._safe_float(row_data[9]),
                                'to_forced': self._safe_float(row_data[10]) if len(row_data) > 10 else 0,
                                
                                # Metadata
                                'source': 'hashtag_basketball',
                                'collected_at': datetime.now()
                            }
                            
                            positional_data.append(defense_record)
                            
                        except Exception as e:
                            logger.error(f"❌ Error processing row: {e}")
                            continue
                
                # If we found data, break (assuming first valid table is the main one)
                if positional_data:
                    break
            
            logger.info(f"✅ Extracted {len(positional_data)} positional defense records")
            
            # Show sample of what we collected
            if positional_data:
                sample_positions = {}
                for record in positional_data[:15]:  # Show first 15
                    pos = record['position']
                    if pos not in sample_positions:
                        sample_positions[pos] = []
                    sample_positions[pos].append(f"{record['team_abbrev']} ({record['pts_allowed']:.1f} pts)")
                
                for pos, teams in sample_positions.items():
                    logger.info(f"📊 {pos}: {teams[:3]}...")  # Show first 3 teams per position
            
            return positional_data
            
        except Exception as e:
            logger.error(f"❌ Error parsing positional defense HTML: {e}")
            return []
    
    def _safe_float(self, value):
        """Safely convert value to float, handling various formats"""
        try:
            if isinstance(value, str):
                # Remove common non-numeric characters
                cleaned = value.replace('%', '').replace(',', '').strip()
                return float(cleaned)
            return float(value)
        except (ValueError, TypeError):
            return 0.0

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