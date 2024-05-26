const AGE_DIST = 'data/history/age_dist/000000_0';
const AVG_RATE = 'data/history/avg_rate/000000_0';
const GENDER_DIST = 'data/history/gender_dist/000000_0';
const GENRE_DIST = 'data/history/genre_dist/000000_0';
const GENRE_DIST_AMONG_GENDER = 'data/history/genre_dist_among_gender/000000_0';
const GENRE_DIST_AMONG_OCC = 'data/history/genres_dist_among_occ/000000_0';
const HIGH_RATE_FREQ = 'data/history/high_rate_freq/000000_0';
const JOB_DIST = 'data/history/job_dist/000000_0';
const NUM_USER_TIME = 'data/history/num_user_time/000000_0';
const RATE_DIST_AMONG_AGE = 'data/history/rate_dist_among_age/000000_0';
const RATE_DIST_AMONG_GENRE = 'data/history/rate_dist_among_genre/000000_0';
const RATE_FREQ = 'data/history/rate_freq/000000_0';
const RATING_DIST = 'data/history/rating_dist/000000_0';
const GENRE_POP_TIME = 'data/history/genre_pop_time';
const GENRE_RATE_TIME = 'data/history/genre_rate_time';
const WEEK_N_YEAR = 'data/history/week_n_year/000000_0';

document.addEventListener('DOMContentLoaded', function () {
    // Create chart here
    graphAgeDist();
    graphGenreDist();
    graphGenderDist();
    graphOccupationDist();
    graphRatingDist();
    graphNumUserTime();
    graphRateDistAmongAge();
    graphGenrePopTime();
    graphGenreRateTime();
    graphGenresDistAmongOccupation();
    graphGenreDistAmongGender();
    graphRateDistAmongGenre();
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

async function graphAgeDist() {
    const text = await loadCSV(AGE_DIST);
    const lines = text.split('\n');
    let json = {};

    // Process each line
    lines.forEach(line => {
        // Split the line by comma to get individual values
        const values = line.split(',');
        json[values[0]] = values[1];
    });
    const ctx = document.getElementById('age-dist');

    const data = {
        labels: Object.keys(json),
        datasets: [{
            data: Object.values(json),
            backgroundColor: generateRandomColors(7)
        }]
    };

    const config = {
        type: 'doughnut',
        data: data,
        options: {
            plugins: {
                legend: {
                    position: 'left',
                }
            }
        }
    };

    new Chart(ctx, config);
}

async function makeChart(id, xValues, yValues, type, chartLabel, xLabel, yLabel) {
    const ctx = document.getElementById(id);

    new Chart(ctx, {
        type: type,
        data: {
            labels: xValues,
            datasets: [{
                label: chartLabel,
                data: Object.values(yValues),
                backgroundColor: generateRandomColors(1)
            }]
        },
        options: {
            scales: {
                x: {
                    title: {
                        display: true,
                        text: xLabel
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: yLabel
                    },
                    beginAtZero: true
                }
            }
        }
    });
}

async function graphGenreDist() {
    const text = await loadCSV(GENRE_DIST);
    const values = text.split(',');
    const genres = [
        'Unknown',
        'Action',
        'Adventure',
        'Animation',
        'Children\'s',
        'Comedy',
        'Crime',
        'Documentary',
        'Drama',
        'Fantasy',
        'Film-Noir',
        'Horror',
        'Musical',
        'Mystery',
        'Romance',
        'Sci-Fi',
        'Thriller',
        'War',
        'Western'
    ];
    makeChart('genre-dist', genres, values, 'bar', '# of Movies', 'Genre', '# of Movies');
}

async function graphGenderDist() {
    const text = await loadCSV(GENDER_DIST);
    const lines = text.split('\n');
    let data = {};

    // Process each line
    lines.forEach(line => {
        // Split the line by comma to get individual values
        const values = line.split(',');
        data[values[0]] = values[1];
    });
    
    const graphData = {
        labels: ['Female', 'Male'],
        datasets: [{
            label: '# of People',
            data: Object.values(data),
            backgroundColor: generateRandomColors(2)
        }]
    };
    const config = {
        type: 'pie',
        data: graphData,
        options: {
            plugins: {
                legend: {
                    position: 'left',
                }
            }
        }
    };

    const ctx = document.getElementById('gender-dist');
    new Chart(ctx, config);
}

async function graphOccupationDist() {
    const text = await loadCSV(JOB_DIST);
    const lines = text.split('\n');
    let json = {};

    // Process each line
    lines.forEach(line => {
        // Split the line by comma to get individual values
        const values = line.split(',');
        json[values[0]] = values[1];
    });

    makeChart('occupation-dist', Object.keys(json), Object.values(json), 'bar', '# of People', 'Occupation', '# of People');
}

async function graphRatingDist() {
    const text = await loadCSV(RATING_DIST);
    const lines = text.split('\n');
    let data = {};

    // Process each line
    lines.forEach(line => {
        // Split the line by comma to get individual values
        const values = line.split(',');
        data[values[0]] = values[1];
    });
    makeChart('rating-dist', Object.keys(data), Object.values(data), 'bar', "# of Ratings", 'Rating', '# of Ratings');
}

async function graphNumUserTime() {
    const text = await loadCSV(NUM_USER_TIME);
    const lines = text.split('\n');
    let data = {};

    // Process each line
    lines.forEach(line => {
        // Split the line by comma to get individual values
        const values = line.split(',');
        let time = values[1] + " - " + values[2];
        data[time] = values[0];
    });
    makeChart('num-user-time', Object.keys(data), Object.values(data), 'line', "# of Users", 'Week No. - Year', '# of Users');
}

async function graphRateDistAmongAge() {
    const text = await loadCSV(RATE_DIST_AMONG_AGE);
    const genres = [
        'Action',
        'Adventure',
        'Animation',
        'Children\'s',
        'Comedy',
        'Crime',
        'Documentary',
        'Drama',
        'Fantasy',
        'Film-Noir',
        'Horror',
        'Musical',
        'Mystery',
        'Romance',
        'Sci-Fi',
        'Thriller',
        'War',
        'Western'
    ];
    const lines = text.split('\n');
    let json = {};

    lines.forEach(line => {
        // Split the line by comma to get individual values
        const values = line.split(',');
        let numData = [];
        for (var i = 1; i <= genres.length; i++) {
            numData.push(values[i]);
        }
        json[values[0]] = numData;
    });
    
    const keys = Object.keys(json);
    const values = Object.values(json);
    const data = {
        labels: genres,
        datasets: [{
            label: keys[0],
            data: values[0],
            backgroundColor: 'rgba(255, 99, 132, 0.8)',
        }, {
            label: keys[1],
            data: values[1],
            backgroundColor: 'rgba(54, 162, 235, 0.6)',
        }, {
            label: keys[2],
            data: values[2],
            backgroundColor: 'rgba(181, 27, 117, 0.6)',
        }, {
            label: keys[3],
            data: values[3],
            backgroundColor: 'rgba(230, 92, 25, 0.6)',
        }, {
            label: keys[4],
            data: values[4],
            backgroundColor: 'rgba(79, 111, 82, 0.6)',
        }, {
            label: keys[5],
            data: values[5],
            backgroundColor: 'rgba(108, 3, 69, 0.6)',
        }, {
            label: keys[6],
            data: values[6],
            backgroundColor: 'rgba(73, 105, 137, 0.6)',
        }]
    };

    const config = {
        type: 'bar',
        data: data,
        options: {
            indexAxis: 'x', // Horizontal bars
            plugins: {
                legend: {
                    position: 'top',
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: '# of Ratings'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Genre'
                    }
                }
            }
        }
    };

    // Create the chart
    new Chart(
        document.getElementById('rate-dist-among-age'),
        config
    );
}

function generateRandomColors(numColors, alpha = 0.8) {
    const colors = [];
    for (let i = 0; i < numColors; i++) {
        const randomColor = `rgba(${Math.floor(Math.random() * 256)}, ${Math.floor(Math.random() * 256)}, ${Math.floor(Math.random() * 256)}, ${alpha})`;
        colors.push(randomColor);
    }
    return colors;
}

async function graphGenrePopTime() {
    let json = {};
    let date = [];
    let dataset = [];
    const genres = [
        'action',
        'adventure',
        'animation',
        'children',
        'comedy',
        'crime',
        'documentary',
        'drama',
        'fantasy',
        'film_noir',
        'horror',
        'musical',
        'mystery',
        'romance',
        'scifi',
        'thriller',
        'war',
        'western'
    ]
    genres.forEach(async genre => {
        const path = GENRE_POP_TIME + '/' + genre + '/000000_0'; 
        const text = await loadCSV(path);
        const lines = text.split('\n');
        let views = [];
        lines.forEach(line => {
            let fields = line.split(',');
            views.push(fields[0]);
        })
        json[genre] = views;
    });
    
    const text2 = await loadCSV(WEEK_N_YEAR);
    const lines2 = text2.split('\n');

    lines2.forEach(line => {
        let fields = line.split(',');
        date.push(fields[0] + " - " + fields[1]);
    });

    const ctx = document.getElementById('genre-pop-time');
    console.log(ctx);

    Object.keys(json).forEach(key => {
        dataset.push({
            label: key,
            data: json[key],
            borderColor: generateRandomColors(1),
            fill: false,
            tension: 0.1
        });
    });
    
    const data = {
        labels: date,
        datasets: dataset
    };

    const config = {
        type: 'line',
        data: data,
        options: {
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    };

    new Chart(ctx, config);
}

async function graphGenreRateTime() {
    let json = {};
    let date = [];
    let dataset = [];
    const genres = [
        'action',
        'adventure',
        'animation',
        'children',
        'comedy',
        'crime',
        'documentary',
        'drama',
        'fantasy',
        'film_noir',
        'horror',
        'musical',
        'mystery',
        'romance',
        'scifi',
        'thriller',
        'war',
        'western'
    ];
    genres.forEach(async genre => {
        const path = GENRE_RATE_TIME + '/' + genre + '/000000_0'; 
        const text = await loadCSV(path);
        const lines = text.split('\n');
        let views = [];
        lines.forEach(line => {
            let fields = line.split(',');
            views.push(fields[0]);
        })
        json[genre] = views;
    });
    
    const text2 = await loadCSV(WEEK_N_YEAR);
    const lines2 = text2.split('\n');

    lines2.forEach(line => {
        let fields = line.split(',');
        date.push(fields[0] + " - " + fields[1]);
    });

    const ctx = document.getElementById('genre-rate-time');
    console.log(ctx);

    Object.keys(json).forEach(key => {
        dataset.push({
            label: key,
            data: json[key],
            borderColor: generateRandomColors(1),
            fill: false,
            tension: 0.1
        });
    });
    
    const data = {
        labels: date,
        datasets: dataset
    };

    const config = {
        type: 'line',
        data: data,
        options: {
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    };

    new Chart(ctx, config);
}

async function graphGenresDistAmongOccupation() {
    const json = {};
    const genres = [
        'action',
        'adventure',
        'animation',
        'children',
        'comedy',
        'crime',
        'documentary',
        'drama',
        'fantasy',
        'film_noir',
        'horror',
        'musical',
        'mystery',
        'romance',
        'scifi',
        'thriller',
        'war',
        'western'
    ];
    const text = await loadCSV(GENRE_DIST_AMONG_OCC);
    const lines = text.split('\n');
    lines.forEach(line => {
        fields = line.split(',');
        let values = [];
        for (var i = 1; i <= 18; i++) {
            values.push(fields[i]);
        }
        json[fields[0]] = values;
    });
    createTabs(json);
    createTabPanes(json);
    Object.keys(json).forEach(key => {
        let ctx = document.getElementById(key + "-canvas");
        let data = {
            labels: genres,
            datasets: [{
                label: "# of ratings",
                data: json[key],
                backgroundColor: generateRandomColors(1)
            }]
        };
        let config = {
            type: 'bar',
            data: data,
            options: {
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: "# of Ratings",
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: "Genre"
                        }
                    }
                },
            }
        };

        new Chart(ctx, config);
    });
}

function createTabs(json) {
    const ul = document.getElementById("genre-dist-among-occupation");
    var first = true;
    Object.keys(json).forEach(key => {
        let li = document.createElement('li');
        li.classList.add('nav-item');
        li.role = "presentation";
        
        let btn = document.createElement('btn');
        btn.classList.add('nav-link');
        if (first == true) {
            btn.classList.add('active');
            first = false;
        }
        btn.setAttribute('data-bs-toggle', 'pill');
        btn.setAttribute('data-bs-target', "#" + key + '-pane');
        btn.setAttribute('type', "button");
        btn.textContent = key;
        li.appendChild(btn);
        ul.appendChild(li);
    });
}

function createTabPanes(json) {
    const panes = document.getElementById('genre-dist-among-occupation-panes');
    var first = true;
    Object.keys(json).forEach(key => {
        let div = document.createElement('div');
        div.classList.add('tab-pane');
        
        if (first == true) {
            div.classList.add('active');
            first = false
        }
        div.setAttribute("tabindex", "0");
        div.id = key + "-pane";

        const canvas = document.createElement("canvas");
        canvas.id = key + "-canvas";
        div.appendChild(canvas);
        panes.appendChild(div);
    });
}

async function graphGenreDistAmongGender() {
    const ctx = document.getElementById('genre-dist-among-gender');
    const genres = [
        'action',
        'adventure',
        'animation',
        'children',
        'comedy',
        'crime',
        'documentary',
        'drama',
        'fantasy',
        'film_noir',
        'horror',
        'musical',
        'mystery',
        'romance',
        'scifi',
        'thriller',
        'war',
        'western'
    ];
    const text = await loadCSV(GENRE_DIST_AMONG_GENDER);
    const lines = text.split('\n');
    const json = {};
    lines.forEach(line => {
        let values = [];
        let fields = line.split(',');
        for (var i = 1; i <= genres.length; i++) {
            values.push(fields[i]);
        }
        json[fields[0]] = values;
    });
    console.log(json);
    let data = {
        labels: genres,
        datasets: [
            {
                label: "Female",
                data: json["F"],
                backgroundColor: generateRandomColors(1)
            },
            {
                label: "Male",
                data: json["M"],
                backgroundColor: generateRandomColors(1)
            }
        ]
    }
    let config = {
        type: 'bar',
        data: data
    }

    new Chart(ctx, config);
}

async function graphRateDistAmongGenre() {
    const text = await loadCSV(RATE_DIST_AMONG_GENRE);
    const fields = text.split(',');
    const genres = [
        'action',
        'adventure',
        'animation',
        'children',
        'comedy',
        'crime',
        'documentary',
        'drama',
        'fantasy',
        'film_noir',
        'horror',
        'musical',
        'mystery',
        'romance',
        'scifi',
        'thriller',
        'war',
        'western'
    ];
    const ctx = document.getElementById('rate-dist-among-genre');
    const data = {
        labels: genres,
        datasets: [
            {
                label: "# of ratings",
                data: fields,
                backgroundColor: generateRandomColors(1)
            }
        ]
    };
    const config = {
        type: 'bar',
        data: data
    }
    new Chart(ctx, config);
}