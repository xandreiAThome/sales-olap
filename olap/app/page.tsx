'use client'

import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  LineChart, Line, PieChart, Pie, Cell
} from 'recharts';

export default function Home() {
  // --- Sample data ---
  const barData = [
    { month: 'Jan', sales: 4000 },
    { month: 'Feb', sales: 3000 },
    { month: 'Mar', sales: 5000 },
    { month: 'Apr', sales: 7000 },
    { month: 'May', sales: 6000 },
  ];

  const lineData = [
    { month: 'Jan', revenue: 2400 },
    { month: 'Feb', revenue: 3200 },
    { month: 'Mar', revenue: 4100 },
    { month: 'Apr', revenue: 5800 },
    { month: 'May', revenue: 6900 },
  ];

  const pieData = [
    { name: 'Electronics', value: 400 },
    { name: 'Clothing', value: 300 },
    { name: 'Groceries', value: 300 },
    { name: 'Accessories', value: 200 },
  ];

  const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444'];

  // --- JSX ---
  return (
    <div className="min-h-screen p-24">
      <div className="flex flex-col items-center">
        <h1 className="text-4xl font-bold mb-8">Sales OLAP Dashboard</h1>

        {/* Rollup Report - Bar Chart */}
        <div className="bg-white p-6 rounded-2xl shadow w-full max-w-4xl mb-12">
          <h2 className="text-2xl font-bold mb-4">📊 Rollup Report - Monthly Sales</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={barData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="month" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Bar dataKey="sales" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Drill Down Report - Line Chart */}
        <div className="bg-white p-6 rounded-2xl shadow w-full max-w-4xl mb-12">
          <h2 className="text-2xl font-bold mb-4">📈 Drill Down Report - Revenue Trend</h2>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={lineData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="month" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="revenue" stroke="#10b981" strokeWidth={3} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Slice Report - Pie Chart */}
        <div className="bg-white p-6 rounded-2xl shadow w-full max-w-4xl">
          <h2 className="text-2xl font-bold mb-4">🥧 Slice Report - Sales by Category</h2>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={pieData}
                dataKey="value"
                nameKey="name"
                outerRadius={100}
                label
              >
                {pieData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
