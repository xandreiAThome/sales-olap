'use client'

import useSWR from 'swr';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  LineChart, Line, PieChart, Pie, Cell
} from 'recharts';

// Fetcher function for SWR
const fetcher = async (url: string) => {
  const res = await fetch(url);
  if (!res.ok) throw new Error('Failed to fetch');
  return res.json();
};

export default function Home() {
  // SWR fetch hooks (with caching)
  const { data: rollupData, error: rollupError } = useSWR('http://localhost:4000/api/rollup', fetcher);
  // const { data: drillDownData, error: drillDownError } = useSWR('http://localhost:4000/api/drillDown', fetcher);
  // const { data: sliceData, error: sliceError } = useSWR('http://localhost:4000/api/slice/East Kobe', fetcher);

  console.log('Rollup Data:', rollupData);

  const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444'];

  return (
    <div className="min-h-screen p-24 bg-gray-100">
      <div className="flex flex-col items-center">
        <h1 className="text-4xl font-bold mb-8">Sales OLAP Dashboard</h1>

        {/* Slice Report - Pie Chart */}
        {/* <div className="bg-white p-6 rounded-2xl shadow w-full max-w-4xl mb-12">
          <h2 className="text-2xl font-bold mb-4">📊 Rollup Report - Monthly Sales</h2>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={sliceData}
                cx="50%"
                cy="50%"
                labelLine={false}
                // label={({ Name, percent }) => `${Name} ${(Number(percent) * 100).toFixed(0)}%`}
                outerRadius={120}
                fill="#8884d8"
                dataKey="total_revenue"   // ✅ FIXED
                nameKey="Name"            // ✅ Tells Recharts which field to use for labels
              >
                {sliceData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div> */}

        {/* Rollup Report - Bar Chart */}
        <div className="bg-white p-6 rounded-2xl shadow w-full max-w-4xl mb-12">
          <h2 className="text-2xl font-bold mb-4">📊 Rollup Report - Monthly Sales</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={rollupData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="month" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Bar dataKey="total_revenue" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
