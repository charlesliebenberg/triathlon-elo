// Main D3.js visualization for Triathlon Elo Rankings

// Global variables
let monthlyData = [];
let currentMonthIndex = 0;
let isPlaying = false;
let playInterval;
let colorScale;
let athleteIdToColor = new Map();
let highlightedAthlete = null;
let processedData = null;

// Dimensions and margins for the chart
const margin = { top: 40, right: 80, bottom: 100, left: 80 }; // Increased bottom margin for rotated labels
const width = document.getElementById('chart').clientWidth - margin.left - margin.right;
const height = 500 - margin.top - margin.bottom;

// Create SVG element
const svg = d3.select('#chart')
  .append('svg')
  .attr('width', width + margin.left + margin.right)
  .attr('height', height + margin.top + margin.bottom)
  .append('g')
  .attr('transform', `translate(${margin.left},${margin.top})`);

// Create groups for various chart elements
const xAxisGroup = svg.append('g')
  .attr('class', 'x-axis')
  .attr('transform', `translate(0,${height})`);

const yAxisGroup = svg.append('g')
  .attr('class', 'y-axis');

const linesGroup = svg.append('g')
  .attr('class', 'lines-group');

const pointsGroup = svg.append('g')
  .attr('class', 'points-group');

// Scales
const xScale = d3.scaleTime()
  .range([0, width]);

const yScale = d3.scaleLinear()
  .range([height, 0]);

// Line generator
const lineGenerator = d3.line()
  .x(d => xScale(new Date(d.date)))
  .y(d => yScale(d.rating))
  .curve(d3.curveMonotoneX);

// Fetch data and initialize visualization
async function fetchMonthlyData() {
  try {
    const response = await fetch('/monthly-data');
    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.message || `HTTP error! Status: ${response.status}`);
    }
    
    monthlyData = await response.json();
    
    if (!monthlyData || monthlyData.length === 0) {
      throw new Error('No data available. Please make sure to run generate_data.py first.');
    }
    
    console.log(`Loaded data for ${monthlyData.length} months`);
    initializeVisualization();
    
  } catch (error) {
    console.error('Error fetching data:', error);
    document.getElementById('chart').innerHTML = 
      `<div class="error-message">
        <h3>Error loading data</h3>
        <p>${error.message || 'An unknown error occurred'}</p>
        <p>If this is the first time running the app, make sure to run <code>python generate_data.py</code> first.</p>
      </div>`;
  }
}

// Initialize the visualization
function initializeVisualization() {
  if (monthlyData.length === 0) {
    console.error('No data available');
    return;
  }

  // Process data to track athletes over time
  processedData = processDataForVisualization();
  
  // Set up scales
  setupScales(processedData);
  
  // Set up color scale
  setupColorScale(processedData.athleteList);
  
  // Initialize slider
  setupSlider();
  
  // Create initial visualization
  updateVisualization(currentMonthIndex);
  
  // Set up play/pause button
  setupPlayButton();
  
  // Create legend
  createLegend(processedData.athleteList);
  
  // Set up event handlers for interactive elements
  setupEventHandlers();
}

// Process data for visualization
function processDataForVisualization() {
  // Get all unique athletes across all months
  const allAthletes = new Set();
  
  monthlyData.forEach(monthData => {
    monthData.athletes.forEach(athlete => {
      allAthletes.add(athlete.id);
    });
  });
  
  // Create a structure to track each athlete's ratings over time
  const athleteRatings = {};
  const athleteList = [];
  
  allAthletes.forEach(id => {
    athleteRatings[id] = {
      id: id,
      name: null,
      country: null,
      flag: null,
      ratings: []
    };
  });
  
  // Fill in the ratings and metadata for each athlete
  monthlyData.forEach(monthData => {
    const monthDate = monthData.month;
    
    monthData.athletes.forEach(athlete => {
      if (!athleteRatings[athlete.id].name) {
        athleteRatings[athlete.id].name = athlete.name;
        athleteRatings[athlete.id].country = athlete.country;
        athleteRatings[athlete.id].flag = athlete.flag;
      }
      
      athleteRatings[athlete.id].ratings.push({
        date: athlete.date,
        monthDate: monthDate,
        rating: athlete.rating,
        event: athlete.event
      });
    });
  });
  
  // Convert to array and sort by highest current rating
  Object.values(athleteRatings).forEach(athlete => {
    if (athlete.ratings.length > 0) {
      // Sort ratings by date
      athlete.ratings.sort((a, b) => new Date(a.date) - new Date(b.date));
      athleteList.push(athlete);
    }
  });
  
  // Sort athleteList by highest final rating
  athleteList.sort((a, b) => {
    const aLastRating = a.ratings[a.ratings.length - 1].rating;
    const bLastRating = b.ratings[b.ratings.length - 1].rating;
    return bLastRating - aLastRating;
  });
  
  return {
    athleteRatings,
    athleteList
  };
}

// Set up scales based on data
function setupScales(processedData) {
  // Find min and max dates
  const firstMonth = monthlyData[0].month;
  const lastMonth = monthlyData[monthlyData.length - 1].month;
  
  // Add one month buffer on each side for better visualization
  const startDate = new Date(firstMonth + '-01');
  const endDate = new Date(lastMonth + '-28');  // Using 28 as a safe day for all months
  startDate.setMonth(startDate.getMonth() - 1);
  endDate.setMonth(endDate.getMonth() + 1);
  
  xScale.domain([startDate, endDate]);
  
  // Find min and max ratings
  let minRating = Infinity;
  let maxRating = -Infinity;
  
  Object.values(processedData.athleteRatings).forEach(athlete => {
    athlete.ratings.forEach(r => {
      minRating = Math.min(minRating, r.rating);
      maxRating = Math.max(maxRating, r.rating);
    });
  });
  
  // Add some padding to the rating scale
  const ratingPadding = (maxRating - minRating) * 0.1;
  yScale.domain([minRating - ratingPadding, maxRating + ratingPadding]);
  
  // Create axes
  const xAxis = d3.axisBottom(xScale)
    .ticks(d3.timeMonth.every(2))
    .tickFormat(d3.timeFormat('%b %Y'));
    
  // Make sure the x-axis labels don't overlap
  xAxisGroup.call(xAxis)
    .selectAll("text")
    .style("text-anchor", "end")
    .attr("dx", "-.8em")
    .attr("dy", ".15em")
    .attr("transform", "rotate(-45)");
  
  const yAxis = d3.axisLeft(yScale)
    .ticks(10);
  
  // Add y-axis to chart (x-axis is already added above)
  yAxisGroup.call(yAxis);
  
  // Label axes
  svg.append('text')
    .attr('class', 'x-axis-label')
    .attr('x', width / 2)
    .attr('y', height + margin.bottom - 20) // Adjusted to account for rotated labels
    .attr('text-anchor', 'middle')
    .text('Date');
  
  svg.append('text')
    .attr('class', 'y-axis-label')
    .attr('transform', 'rotate(-90)')
    .attr('x', -height / 2)
    .attr('y', -margin.left + 20)
    .attr('text-anchor', 'middle')
    .text('Elo Rating');
  
  // Set timeline labels
  document.getElementById('start-date').textContent = d3.timeFormat('%b %Y')(startDate);
  document.getElementById('end-date').textContent = d3.timeFormat('%b %Y')(endDate);
}

// Set up color scale
function setupColorScale(athleteList) {
  // Use D3's category colors but limit to 20 colors 
  // (we'll recycle colors for athletes outside the top 20)
  colorScale = d3.scaleOrdinal(d3.schemeCategory10);
  
  // Assign colors to top athletes to keep them consistent
  athleteList.slice(0, 20).forEach((athlete, i) => {
    athleteIdToColor.set(athlete.id, colorScale(i));
  });
}

// Set up slider
function setupSlider() {
  const slider = document.getElementById('timeline-slider');
  slider.min = 0;
  slider.max = monthlyData.length - 1;
  slider.value = 0;
  
  slider.addEventListener('input', function() {
    currentMonthIndex = parseInt(this.value);
    updateVisualization(currentMonthIndex);
  });
}

// Set up play button
function setupPlayButton() {
  const playButton = document.getElementById('play-button');
  
  playButton.addEventListener('click', function() {
    if (isPlaying) {
      // Stop playing
      clearInterval(playInterval);
      playButton.textContent = 'Play';
    } else {
      // Start playing from current position
      playButton.textContent = 'Pause';
      playInterval = setInterval(() => {
        if (currentMonthIndex < monthlyData.length - 1) {
          currentMonthIndex++;
          document.getElementById('timeline-slider').value = currentMonthIndex;
          updateVisualization(currentMonthIndex);
        } else {
          // Reached the end, stop playing
          clearInterval(playInterval);
          playButton.textContent = 'Play';
          isPlaying = false;
        }
      }, 1000); // Advance every second
    }
    
    isPlaying = !isPlaying;
  });
}

// Create the legend
function createLegend(athleteList) {
  const legendContainer = document.getElementById('legend');
  legendContainer.innerHTML = '';
  
  // Use top 10 athletes for the legend (or fewer if less exist)
  const topAthletes = athleteList.length > 10 ? athleteList.slice(0, 10) : athleteList;
  
  topAthletes.forEach(athlete => {
    const legendItem = document.createElement('div');
    legendItem.className = 'legend-item';
    legendItem.dataset.athleteId = athlete.id;
    
    const colorBox = document.createElement('div');
    colorBox.className = 'legend-color';
    colorBox.style.backgroundColor = athleteIdToColor.get(athlete.id) || colorScale(athlete.id);
    
    const nameLabel = document.createElement('span');
    nameLabel.textContent = athlete.name;
    
    legendItem.appendChild(colorBox);
    legendItem.appendChild(nameLabel);
    legendContainer.appendChild(legendItem);
    
    // Add highlighting on hover
    legendItem.addEventListener('mouseenter', () => {
      highlightAthlete(athlete.id);
    });
    
    legendItem.addEventListener('mouseleave', () => {
      unhighlightAthlete();
    });
  });
}

// Set up event handlers
function setupEventHandlers() {
  // Window resize handler
  window.addEventListener('resize', debounce(() => {
    // Update width based on container size
    const newWidth = document.getElementById('chart').clientWidth - margin.left - margin.right;
    
    // Only redraw if width has changed significantly
    if (Math.abs(newWidth - width) > 50) {
      // This would need to update the SVG, scales, etc.
      // For simplicity, we're just reloading here
      location.reload();
    }
  }, 250));
}

// Update visualization for the given month index
function updateVisualization(monthIndex) {
  if (!monthlyData || monthlyData.length === 0 || !processedData) {
    console.error('Data not ready for visualization update');
    return;
  }

  const currentMonthData = monthlyData[monthIndex];
  
  // Update the current date display
  document.getElementById('current-date').textContent = formatMonthDate(currentMonthData.month);
  
  // Get the athletes in the top 10 for this month
  const topAthletes = currentMonthData.athletes;
  
  // Get athlete IDs in the current top 10
  const currentTopIds = new Set(topAthletes.map(a => a.id));
  
  // Update lines
  updateLines(currentTopIds);
  
  // Update points
  updatePoints(topAthletes);
  
  // Update #1 athlete display
  updateTopAthleteDisplay(topAthletes[0]);
}

// Update lines
function updateLines(currentTopIds) {
  // Select all lines
  const lines = linesGroup.selectAll('.athlete-line')
    .data(Object.values(processedData.athleteRatings), d => d.id);
  
  // Update existing lines
  lines
    .attr('d', d => lineGenerator(d.ratings))
    .attr('stroke', d => athleteIdToColor.get(d.id) || colorScale(d.id))
    .style('opacity', d => {
      if (highlightedAthlete !== null) {
        return d.id === highlightedAthlete ? 1 : 0.1;
      }
      return currentTopIds.has(d.id) ? 1 : 0.1;
    });
  
  // Add new lines
  lines.enter()
    .append('path')
    .attr('class', 'athlete-line')
    .attr('id', d => `line-${d.id}`)
    .attr('d', d => lineGenerator(d.ratings))
    .attr('stroke', d => athleteIdToColor.get(d.id) || colorScale(d.id))
    .style('opacity', d => {
      if (highlightedAthlete !== null) {
        return d.id === highlightedAthlete ? 1 : 0.1;
      }
      return currentTopIds.has(d.id) ? 1 : 0.1;
    });
  
  // Remove old lines (should not happen with our data structure)
  lines.exit().remove();
}

// Update points
function updatePoints(topAthletes) {
  // Clear existing points
  pointsGroup.selectAll('.athlete-circle').remove();
  
  // Add points for current top athletes
  pointsGroup.selectAll('.athlete-circle')
    .data(topAthletes)
    .enter()
    .append('circle')
    .attr('class', 'athlete-circle')
    .attr('cx', d => xScale(new Date(d.date)))
    .attr('cy', d => yScale(d.rating))
    .attr('r', 6)
    .attr('fill', d => athleteIdToColor.get(d.id) || colorScale(d.id))
    .style('stroke', '#fff')
    .style('stroke-width', 2)
    .on('mouseenter', function(event, d) {
      showTooltip(event, d);
      highlightAthlete(d.id);
    })
    .on('mousemove', function(event, d) {
      moveTooltip(event);
    })
    .on('mouseleave', function() {
      hideTooltip();
      unhighlightAthlete();
    });
}

// Update top athlete display
function updateTopAthleteDisplay(athlete) {
  if (!athlete) return;
  
  // Set name
  document.getElementById('top-athlete-name').textContent = athlete.name;
  
  // Set rating
  document.getElementById('top-athlete-rating').textContent = `Rating: ${Math.round(athlete.rating)}`;
  
  // Set flag
  const flagImg = document.getElementById('top-athlete-flag');
  flagImg.src = athlete.flag;
  flagImg.alt = `${athlete.country} flag`;
  
  // Set photo using the profile image URL from the data
  document.getElementById('top-athlete-photo').src = athlete.profile_image;
  
  // Calculate tenure if possible (how many consecutive months as #1)
  let tenure = 1;
  let currentId = athlete.id;
  
  for (let i = currentMonthIndex - 1; i >= 0; i--) {
    const prevTopAthlete = monthlyData[i].athletes[0];
    if (prevTopAthlete.id === currentId) {
      tenure++;
    } else {
      break;
    }
  }
  
  if (tenure === 1) {
    document.getElementById('top-athlete-tenure').textContent = 'New #1 this month!';
  } else {
    document.getElementById('top-athlete-tenure').textContent = `#1 for ${tenure} months`;
  }
}

// Tooltip functions
function showTooltip(event, athlete) {
  const tooltip = document.getElementById('tooltip');
  
  tooltip.innerHTML = `
    <div class="tooltip-header">
      <img src="${athlete.flag}" alt="${athlete.country} flag">
      <span class="tooltip-name">${athlete.name}</span>
    </div>
    <div class="tooltip-photo">
      <img src="${athlete.profile_image}" alt="${athlete.name}" class="tooltip-athlete-photo">
    </div>
    <div class="tooltip-rating">Rating: ${Math.round(athlete.rating)}</div>
    <div class="tooltip-rank">Rank: #${monthlyData[currentMonthIndex].athletes.findIndex(a => a.id === athlete.id) + 1}</div>
    <div>Last event: ${athlete.event}</div>
  `;
  
  tooltip.style.opacity = 1;
  moveTooltip(event);
}

function moveTooltip(event) {
  const tooltip = document.getElementById('tooltip');
  
  // Position the tooltip near but not on the cursor
  const tooltipX = event.pageX + 15;
  const tooltipY = event.pageY - 30;
  
  tooltip.style.left = `${tooltipX}px`;
  tooltip.style.top = `${tooltipY}px`;
}

function hideTooltip() {
  document.getElementById('tooltip').style.opacity = 0;
}

// Athlete highlighting
function highlightAthlete(athleteId) {
  highlightedAthlete = athleteId;
  
  // Highlight the relevant line
  linesGroup.selectAll('.athlete-line')
    .style('opacity', d => d.id === athleteId ? 1 : 0.1)
    .style('stroke-width', d => d.id === athleteId ? 3 : 2);
  
  // Highlight the legend item
  document.querySelectorAll('.legend-item').forEach(item => {
    if (parseInt(item.dataset.athleteId) === athleteId) {
      item.style.fontWeight = 'bold';
    } else {
      item.style.opacity = 0.5;
    }
  });
}

function unhighlightAthlete() {
  highlightedAthlete = null;
  
  // Restore all lines
  const currentTopIds = new Set(monthlyData[currentMonthIndex].athletes.map(a => a.id));
  
  linesGroup.selectAll('.athlete-line')
    .style('opacity', d => currentTopIds.has(d.id) ? 1 : 0.1)
    .style('stroke-width', 2);
  
  // Restore legend
  document.querySelectorAll('.legend-item').forEach(item => {
    item.style.fontWeight = 'normal';
    item.style.opacity = 1;
  });
}

// Helper functions
function formatMonthDate(yearMonth) {
  const [year, month] = yearMonth.split('-');
  const date = new Date(parseInt(year), parseInt(month) - 1);
  return date.toLocaleString('default', { month: 'long', year: 'numeric' });
}

// Debounce function for performance
function debounce(func, wait) {
  let timeout;
  return function() {
    const context = this;
    const args = arguments;
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(context, args), wait);
  };
}

// Initialize visualization when the page loads
document.addEventListener('DOMContentLoaded', fetchMonthlyData);