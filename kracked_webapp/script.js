const ctx = document.getElementById('priceChart').getContext('2d');

let chart = new Chart(ctx, {
    type: 'line',
    data: {
        labels: [],
        datasets: [{
            label: 'Price',
            data: [],
            borderColor: 'rgba(75, 192, 192, 1)',
            borderWidth: 2,
            fill: false,
        }]
    },
    options: {
        scales: {
            x: {
                type: 'linear',
                position: 'bottom',
                title: {
                    display: true,
                    text: 'Time'
                }
            },
            y: {
                title: {
                    display: true,
                    text: 'Price'
                }
            }
        }
    }
});

function fetchCSV() {
    fetch('data.csv')
        .then(response => response.text())
        .then(data => {
            const rows = data.split('\n').slice(1); // Skip header row
            rows.forEach(row => {
                const [time, price] = row.split(',');
                if (time && price) {
                    chart.data.labels.push(parseFloat(time));
                    chart.data.datasets[0].data.push(parseFloat(price));
                }
            });
            chart.update();
        })
        .catch(error => console.error('Error fetching the CSV file:', error));
}

setInterval(fetchCSV, 1000);

