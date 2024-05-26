// Function to fetch and parse CSV data
async function loadCSV(url) {
    const response = await fetch(url);
    const data = await response.text();
    const rows = data.split('\n');
    const result = [];

    // Loop through each row and split it by comma to create an array
    rows.forEach(row => {
        result.push(row.split(','));
    });

    return result;
}

// Example usage
const csvURL = '/home/minh/Work/ml-project/hive_results/genre_dist/000000_0';
loadCSV(csvURL)
    .then(data => {
        console.log(data); // This will log the CSV data in array format
        // You can perform further operations with the data here
    })
    .catch(error => {
        console.error('Error loading CSV:', error);
    });


function loadGenreDist() {


    const config = {
        type: 'bar',
        data: data,
        options: {
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        },
    };
}