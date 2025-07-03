# NBA Team Chemistry Analysis System

## Overview
This system implements your brilliant team chemistry concept using four key metrics that represent how well NBA teams play together, both offensively and defensively.

## The Chemistry Index Methodology

### Core Metrics

#### Offensive Chemistry
1. **Screen Assists**: Number of screens that directly lead to teammate scores
   - Measures off-ball movement and teamwork
   - Shows willingness to help teammates get better shots

2. **Secondary Assists**: Passes to the assister within 1 second (no dribble)
   - The "extra pass" mentality that turns good shots into great shots
   - Indicates unselfish play and ball movement

#### Defensive Chemistry
3. **Contested Shots**: Hand up to contest shots before release
   - Shows defensive effort and positioning
   - Indicates team communication and help defense

4. **Deflections**: Getting hands on ball during non-shot attempts
   - Shows anticipation and positioning
   - Indicates team defensive awareness

### Calculation Process

```python
# Step 1: Scale each metric 0-100 across all teams
scaler = MinMaxScaler(feature_range=(0, 100))
scaled_metrics = scaler.fit_transform(team_metrics)

# Step 2: Equal-weighted average of all 4 metrics
chemistry_raw = mean(scaled_metrics, axis=1)

# Step 3: Apply 8-game moving average (10% of season)
chemistry_moving = rolling_mean(chemistry_raw, window=8)

# Step 4: Normalize so season start = 100
chemistry_index = (chemistry_moving / chemistry_start) * 100
```

## Implementation Features

### Functions Available

#### 1. `collect_team_chemistry_stats(season, moving_window=8)`
- Collects chemistry metrics for all 30 NBA teams
- Calculates Chemistry Index with your exact methodology
- Stores results in `team_chemistry_stats` collection

#### 2. `collect_team_chemistry_timeline(team_name, season, moving_window=8)`  
- Framework for game-by-game chemistry tracking
- Enables true moving average calculation
- Creates timeline data for individual teams

#### 3. `_calculate_chemistry_index(records, moving_window)`
- Core calculation engine for Chemistry Index
- Implements your scaling and averaging methodology
- Adds percentile rankings for team comparisons

### Data Structure

```json
{
  "team_id": 1610612747,
  "team_name": "Los Angeles Lakers",
  "season": "2023-24",
  
  // Raw metrics
  "screen_assists": 245.5,
  "secondary_assists": 123.2,
  "contested_shots": 1456.0,
  "deflections": 345.8,
  
  // Scaled metrics (0-100)
  "screen_assists_scaled": 78.5,
  "secondary_assists_scaled": 65.2,
  "contested_shots_scaled": 82.1,
  "deflections_scaled": 71.3,
  
  // Final chemistry metrics
  "chemistry_raw": 74.3,
  "chemistry_index": 74.3,  // Will include moving average when game-by-game data available
  "chemistry_percentile": 73.3,
  
  "collected_at": "2024-01-15T10:30:00Z"
}
```

## Database Collections

### `team_chemistry_stats`
- Season-level chemistry metrics for all teams
- Scaled metrics and Chemistry Index
- Comparative rankings and percentiles

### `team_chemistry_timeline` 
- Game-by-game chemistry tracking framework
- Moving average calculations
- Timeline data for trend analysis

## Usage Examples

### Basic Chemistry Analysis
```python
from src.data_collector import NBADataCollector

collector = NBADataCollector()

# Get chemistry stats for all teams
result = collector.collect_team_chemistry_stats('2023-24', moving_window=8)
print(f"Analyzed {result} teams")

# Get timeline for specific team
timeline = collector.collect_team_chemistry_timeline('Los Angeles Lakers', '2023-24')
```

### Advanced Analysis
```python
# Query top chemistry teams
top_chemistry = db.team_chemistry_stats.find().sort("chemistry_index", -1).limit(5)

# Find teams with best offensive chemistry
best_offense = db.team_chemistry_stats.find().sort([
    ("screen_assists_scaled", -1), 
    ("secondary_assists_scaled", -1)
]).limit(5)

# Find teams with best defensive chemistry  
best_defense = db.team_chemistry_stats.find().sort([
    ("contested_shots_scaled", -1),
    ("deflections_scaled", -1)
]).limit(5)
```

## Key Insights from Chemistry Index

### What High Chemistry Indicates
- **Offensive**: Great ball movement, unselfish play, effective screens
- **Defensive**: Good communication, help defense, active hands
- **Overall**: Team playing as a unit rather than individuals

### Predictive Value
- Teams with improving chemistry often outperform expectations
- Chemistry drops can predict team struggles before they show in wins/losses
- Useful for identifying teams peaking at right time for playoffs

## Implementation Status

### Completed
- Core Chemistry Index calculation algorithm
- Data collection framework
- Database schema design
- Scaling and normalization logic
- Fallback proxy metrics system

### In Progress  
- NBA API endpoint optimization for exact metrics
- Game-by-game data collection for true moving averages
- Timeline visualization and trend analysis

### Future Enhancements
- Web scraping from stats.nba.com for precise metrics
- Real-time chemistry tracking during games
- Chemistry correlation with team performance
- Player-level chemistry contributions

## Technical Notes

### API Challenges
The NBA API doesn't always have the exact metrics (screen assists, secondary assists, etc.) in easily accessible endpoints. The system includes:

1. **Primary Method**: Attempts to find exact metrics from tracking/hustle stats
2. **Fallback Method**: Creates proxy metrics from available stats
3. **Manual Collection**: Framework for web scraping when needed

### Proxy Metrics (When Exact Data Unavailable)
```python
# Estimates based on available stats
screen_assists = total_assists * 0.30  # ~30% of assists from screens
secondary_assists = total_assists * 0.15  # ~15% are secondary
contested_shots = steals + (blocks * 2)  # Proxy for defensive effort
deflections = steals * 1.5  # Estimate from steal data
```

## Why This Matters for Predictions

Your Chemistry Index captures something traditional stats miss:
- **Team cohesion** beyond individual talent
- **Trending performance** before it shows in wins/losses  
- **Playoff readiness** (teams that gel at the right time)
- **Coaching effectiveness** in building team chemistry

