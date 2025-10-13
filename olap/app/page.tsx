'use client'

import useSWR from 'swr';
import {
  PieChart, Pie, Cell, Tooltip, ResponsiveContainer, Legend
} from 'recharts';

const fetcher = async (url: string) => {
  const res = await fetch(url);
  if (!res.ok) throw new Error('Failed to fetch');
  return res.json();
};

export default function Home() {
  const { data: sliceData, error: sliceError } = useSWR('http://localhost:4000/api/slice/East Kobe', fetcher);

  if (sliceError) return <div className="p-24">Failed to load data 😢</div>;
  if (!sliceData) return <div className="p-24">Loading data...</div>;

  const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444'];

  return (
    <div className="min-h-screen p-24 bg-gray-100">
      <div className="flex flex-col items-center">
        <h1 className="text-4xl font-bold mb-8">Sales OLAP Dashboard</h1>

        <div className="bg-white p-6 rounded-2xl shadow w-full max-w-auto mb-12">
          <h2 className="text-2xl font-bold mb-4">📊 Slice Report - City Breakdown</h2>

          <ResponsiveContainer width="100%" height={650}>
            <PieChart>
              <Pie
                data={sliceData}
                cx="50%"
                cy="50%"
                outerRadius={120}
                dataKey="total_revenue"
                nameKey="Name"
                labelLine={false} // remove labels
              >
                {sliceData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>

              {/* Tooltip for hover info */}
              <Tooltip 
                formatter={(value: number) => value.toLocaleString()} 
                labelFormatter={(name: string) => `Category: ${name}`} 
              />

              {/* Legend replaces labels */}
              <Legend 
                verticalAlign="bottom" 
                align="center" 
                iconType="circle" 
                layout="horizontal" 
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
