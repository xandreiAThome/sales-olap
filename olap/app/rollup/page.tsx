'use client';

import { useState } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts';
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import useSWR from 'swr';
import { fetcher } from '@/utils/fetcher';

export default function RollupReport() {
  const { data: rollupData, error: rollupError } = useSWR(
    'http://localhost:4000/api/rollup',
    fetcher
  );

  type FilterKey = 'Year' | 'Quarter' | 'Month'; // 👈 match your API keys (case-sensitive)
  const [filterKey, setFilterKey] = useState<FilterKey>('Year');

  // --- Handle loading/error states ---
  if (rollupError) {
    return <div className="p-6 text-red-500 font-semibold">Failed to load data 😢</div>;
  }

  if (!rollupData) {
    return <div className="p-6 text-gray-500 italic">Loading rollup data...</div>;
  }

  // --- Aggregate data ---
  const grouped: Record<string, number> = {};

  rollupData.forEach((item: any) => {
    const key = item[filterKey];
    grouped[key] = (grouped[key] || 0) + item.revenue; // 👈 use correct property
  });

  const aggregatedData = Object.entries(grouped).map(([key, total]) => ({
    [filterKey]: key,
    revenue: total,
  }));

  const xKey = filterKey;

  if (aggregatedData.length === 0) {
    return <div className="p-6 text-gray-500 italic">No data available.</div>;
  }

  return (
    <div className="bg-white p-6 rounded-2xl shadow w-full max-w-auto mb-12">
      <h2 className="text-2xl font-bold mb-6">📊 Rollup Report - Sales Overview</h2>

      {/* --- Filter Selector --- */}
      <div className="flex flex-wrap gap-4 mb-6">
        <Select value={filterKey} onValueChange={(val: FilterKey) => setFilterKey(val)}>
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="Select rollup" />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              <SelectItem value="Year">Year</SelectItem>
              <SelectItem value="Quarter">Quarter</SelectItem>
              <SelectItem value="Month">Month</SelectItem>
            </SelectGroup>
          </SelectContent>
        </Select>
      </div>

      {/* --- Bar Chart --- */}
      <ResponsiveContainer width="70%" height={600}>
        <BarChart
          data={aggregatedData}
          margin={{ top: 20, right: 40, left: 80, bottom: 20 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={xKey} />
          <YAxis
            tickFormatter={(v) => `${(v / 1e9).toFixed(2)}B`} // ✅ display billions
            label={{
              value: 'Revenue (Billion ₱)',
              angle: -90,
              position: 'insideLeft',
              offset: 10,
            }}
          />
          <Tooltip
            formatter={(value: number) =>
              `₱${(value / 1e9).toFixed(2)}B`
            } // ✅ tooltip shows billions
            labelFormatter={(label) => `${filterKey}: ${label}`}
          />
          <Legend />
          <Bar dataKey="revenue" fill="#3b82f6" name="Total Revenue" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
