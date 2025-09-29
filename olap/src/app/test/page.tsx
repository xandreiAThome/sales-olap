"use client";

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
  RadialLinearScale,
} from "chart.js";

import {
  Line,
  Bar,
  Pie,
  Doughnut,
  Radar,
  PolarArea,
  Scatter,
  Bubble,
} from "react-chartjs-2";

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ArcElement,
  RadialLinearScale
);

// Example data
const labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"];

const lineData = {
  labels,
  datasets: [
    {
      label: "Line Dataset",
      data: [10, 20, 15, 30, 25, 40],
      borderColor: "rgb(75, 192, 192)",
      backgroundColor: "rgba(75, 192, 192, 0.2)",
      tension: 0.3,
    },
  ],
};

const barData = {
  labels,
  datasets: [
    {
      label: "Bar Dataset",
      data: [12, 19, 3, 5, 2, 3],
      backgroundColor: "rgba(54, 162, 235, 0.6)",
    },
  ],
};

const pieData = {
  labels: ["Red", "Blue", "Yellow"],
  datasets: [
    {
      data: [300, 50, 100],
      backgroundColor: ["#FF6384", "#36A2EB", "#FFCE56"],
    },
  ],
};

const doughnutData = {
  labels: ["Apple", "Banana", "Cherry"],
  datasets: [
    {
      data: [200, 150, 100],
      backgroundColor: ["#FF6384", "#36A2EB", "#FFCE56"],
    },
  ],
};

const radarData = {
  labels: ["Eating", "Drinking", "Sleeping", "Designing", "Coding", "Running"],
  datasets: [
    {
      label: "Radar Dataset",
      data: [65, 59, 90, 81, 56, 55],
      backgroundColor: "rgba(255, 99, 132, 0.2)",
      borderColor: "rgb(255, 99, 132)",
    },
  ],
};

const polarData = {
  labels: ["Red", "Green", "Yellow", "Grey", "Blue"],
  datasets: [
    {
      data: [11, 16, 7, 3, 14],
      backgroundColor: ["#FF6384", "#4BC0C0", "#FFCE56", "#E7E9ED", "#36A2EB"],
    },
  ],
};

const scatterData = {
  datasets: [
    {
      label: "Scatter Dataset",
      data: [
        { x: -10, y: 0 },
        { x: 0, y: 10 },
        { x: 10, y: 5 },
        { x: 0.5, y: 5.5 },
      ],
      backgroundColor: "rgb(75, 192, 192)",
    },
  ],
};

const bubbleData = {
  datasets: [
    {
      label: "Bubble Dataset",
      data: [
        { x: 20, y: 30, r: 15 },
        { x: 40, y: 10, r: 10 },
      ],
      backgroundColor: "rgba(255, 99, 132, 0.5)",
    },
  ],
};

export default function AllCharts() {
  return (
    <div className="p-4 space-y-8">
      <h2 className="text-xl font-bold">Line Chart</h2>
      <Line data={lineData} />

      <h2 className="text-xl font-bold">Bar Chart</h2>
      <Bar data={barData} />

      <h2 className="text-xl font-bold">Pie Chart</h2>
      <Pie data={pieData} />

      <h2 className="text-xl font-bold">Doughnut Chart</h2>
      <Doughnut data={doughnutData} />

      <h2 className="text-xl font-bold">Radar Chart</h2>
      <Radar data={radarData} />

      <h2 className="text-xl font-bold">Polar Area Chart</h2>
      <PolarArea data={polarData} />

      <h2 className="text-xl font-bold">Scatter Chart</h2>
      <Scatter data={scatterData} />

      <h2 className="text-xl font-bold">Bubble Chart</h2>
      <Bubble data={bubbleData} />
    </div>
  );
}
