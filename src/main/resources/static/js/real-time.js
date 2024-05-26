const VISIT = 'data/speed/visit.txt';
const RATE = 'data/speed/rate.txt';
const REVIEW = 'data/speed/review.txt';
const SEARCH = 'data/speed/search.txt';

var rateChart, visitChart, reviewChart, searchChart;

document.addEventListener('DOMContentLoaded', function () {
    // Create chart here
    graphRate();
    graphVisit();
    graphReview();
    graphSearch();
});

function loadCSV(filepath) {
    return new Promise((resolve, reject) => {
        var xhr = new XMLHttpRequest();
        xhr.onreadystatechange = function () {
            if (xhr.readyState === XMLHttpRequest.DONE) {
                if (xhr.status === 200) {
                    resolve(this.responseText);
                } else {
                    reject(new Error('Failed to load file'));
                }
            }
        };
        xhr.open('GET', filepath, true);
        xhr.send();
    });
}

function createEmptyLineChart(ctx) {
    return new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: '',
                    data: [],
                    fill: false,
                    borderColor: 'rgb(92, 223, 255)',
                    tension: 0.1
                },
                {
                    label: '',
                    data: [],
                    fill: false,
                    borderColor: 'rgb(255, 92, 92)',
                    tension: 0.1
                },
                {
                    label: '',
                    data: [],
                    fill: false,
                    borderColor: 'rgb(0, 204, 0)',
                    tension: 0.1
                },
                {
                    label: '',
                    data: [],
                    fill: false,
                    borderColor: 'rgb(255, 255, 0)',
                    tension: 0.1
                },
                {
                    label: '',
                    data: [],
                    fill: false,
                    borderColor: 'rgb(153, 102, 255)',
                    tension: 0.1
                },
            ]
        },
        options: {
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'second'
                    },
                    display: true,
                    title: {
                        display: true,
                        text: 'Time'
                    }
                },
                y: {
                    beginAtZero: false,
                    display: true,
                    title: {
                        display: true,
                    }
                }
            },
            animation: false,
            datasets: {
                line: {
                    pointRadius: 0 // disable for all `'line'` datasets
                }
            },
            elements: {
                point: {
                    radius: 0 // default to disabled in all datasets
                }
            }
        }
    });
}

function graphSearch() {
    // Initialize an empty chart
    const ctx = document.getElementById('most-keyword-search').getContext('2d');
    searchChart = createEmptyLineChart(ctx);
}

function graphRate() {
    // Initialize an empty chart
    const ctx = document.getElementById('movie-top-rate').getContext('2d');
    rateChart = createEmptyLineChart(ctx);
}

function graphVisit() {
    // Initialize an empty chart
    const ctx = document.getElementById('movie-top-visited').getContext('2d');
    visitChart = createEmptyLineChart(ctx);
}

function graphReview() {
    // Initialize an empty chart
    const ctx = document.getElementById('most-review').getContext('2d');
    reviewChart = createEmptyLineChart(ctx);
}

async function updateSearchChart() {
    const text = await loadCSV(SEARCH);
    if (text.length == 0) {
        return;
    }
    const lines = text.split('\n');
    let jsonList = [];
    lines.forEach(element => {
        console.log(element);
        let fields = element.split("|");
        // console.log(fields);         
        let json = {};
        json['keyword'] = fields[0];
        json['number'] = fields[1];
        jsonList.push(json);
    });

    let jsonSortedList = jsonList.sort((a, b) => {
        return a.keyword - b.keyword;
    });

    jsonList = jsonSortedList;

    let size = jsonList.length;
    for (var i = 0; i < size; i++) {
        // Add a new data point with current timestamp
        searchChart.data.datasets[i].label = jsonList[i]['keyword'];
        searchChart.data.datasets[i].data.push({ x: new Date(), y: jsonList[i]['number'] });

        // Remove oldest data point if exceeding a certain number of points
        if (searchChart.data.datasets[i].data.length > 20) {
            searchChart.data.datasets[i].data.shift();
        }
    }

    // Update the chart
    searchChart.update();
}

async function updateReviewChart() {
    const text = await loadCSV(REVIEW);
    const lines = text.split('\n');
    if (lines[0] == "null") {
        return;
    }
    let jsonList = [];
    lines.forEach(element => {
        let fields = element.split("|");
        let json = {};
        json['movie_id'] = fields[0];
        json['review'] = fields[1];
        jsonList.push(json);
    });

    let jsonSortedList = jsonList.sort((a, b) => {
        return a.movie_id - b.movie_id;
    });

    jsonList = jsonSortedList;

    for (var i = 0; i < 5; i++) {
        // Add a new data point with current timestamp
        reviewChart.data.datasets[i].label = jsonList[i]['movie_id'];
        reviewChart.data.datasets[i].data.push({ x: new Date(), y: jsonList[i]['review'] });

        // Remove oldest data point if exceeding a certain number of points
        if (reviewChart.data.datasets[i].data.length > 20) {
            reviewChart.data.datasets[i].data.shift();
        }
    }

    // Update the chart
    reviewChart.update();
}

async function updateVisitChart() {
    const text = await loadCSV(VISIT);
    const lines = text.split('\n');
    if (lines[0] == "null") {
        return;
    }
    let jsonList = [];
    lines.forEach(element => {
        let fields = element.split("|");
        let json = {};
        json['movie_id'] = fields[0];
        json['visit'] = fields[1];
        jsonList.push(json);
    });

    let jsonSortedList = jsonList.sort((a, b) => {
        return a.movie_id - b.movie_id;
    });

    jsonList = jsonSortedList;

    for (var i = 0; i < 5; i++) {
        // Add a new data point with current timestamp
        visitChart.data.datasets[i].label = jsonList[i]['movie_id'];
        visitChart.data.datasets[i].data.push({ x: new Date(), y: jsonList[i]['visit'] });

        // Remove oldest data point if exceeding a certain number of points
        if (visitChart.data.datasets[i].data.length > 20) {
            visitChart.data.datasets[i].data.shift();
        }
    }

    // Update the chart
    visitChart.update();
}

async function updateRateChart() {
    const text = await loadCSV(RATE);
    const lines = text.split('\n');
    if (lines[0] == "null") {
        return;
    }
    let jsonList = [];
    lines.forEach(element => {
        let fields = element.split("|");
        let json = {};
        json['movie_id'] = fields[0];
        json['rating'] = fields[3];
        jsonList.push(json);
    });

    let jsonSortedList = jsonList.sort((a, b) => {
        return a.movie_id - b.movie_id;
    });

    jsonList = jsonSortedList;

    for (var i = 0; i < 5; i++) {
        // Add a new data point with current timestamp
        rateChart.data.datasets[i].label = jsonList[i]['movie_id'];
        rateChart.data.datasets[i].data.push({ x: new Date(), y: jsonList[i]['rating'] });

        // Remove oldest data point if exceeding a certain number of points
        if (rateChart.data.datasets[i].data.length > 20) {
            rateChart.data.datasets[i].data.shift();
        }
    }

    // Update the chart
    rateChart.update();
}

// Update the chart every second
setInterval(updateRateChart, 1000);
setInterval(updateVisitChart, 1000);
setInterval(updateReviewChart, 1000);
setInterval(updateSearchChart, 1000);
