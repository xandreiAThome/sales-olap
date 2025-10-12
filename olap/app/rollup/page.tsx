'use client';

import { useState, useMemo } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

// Example data
const rollupData = [
  { year: 2023, quarter: 'Q1', month: 'January', total_revenue: 12000 },
  { year: 2023, quarter: 'Q1', month: 'February', total_revenue: 18000 },
  { year: 2023, quarter: 'Q2', month: 'April', total_revenue: 25000 },
  { year: 2024, quarter: 'Q1', month: 'January', total_revenue: 19000 },
  { year: 2024, quarter: 'Q2', month: 'April', total_revenue: 30000 },
];

export default function RollupReport() {
  type FilterKey = 'year' | 'quarter' | 'month';
  const [filterKey, setFilterKey] = useState<FilterKey>('year');

  // --- Aggregate data dynamically ---
  const aggregatedData = useMemo(() => {
    const grouped = rollupData.reduce((acc: Record<string, number>, item) => {
      const key = item[filterKey];
      acc[key] = (acc[key] || 0) + item.total_revenue;
      return acc;
    }, {});

    // Convert to array for Recharts
    return Object.entries(grouped).map(([key, total]) => ({
      [filterKey]: key,
      total_revenue: total,
    }));
  }, [filterKey]);

  // --- Chart axis key (x-axis depends on filterKey) ---
  const xKey = filterKey;

  return (
    <div className="bg-white p-6 rounded-2xl shadow w-full max-w-4xl mb-12">
      <h2 className="text-2xl font-bold mb-6">📊 Rollup Report - Sales Overview</h2>

      {/* --- Filter Selector --- */}
      <div className="flex flex-wrap gap-4 mb-6">
        <Select value={filterKey} onValueChange={(val: FilterKey) => setFilterKey(val)}>
          <SelectTrigger className="w-[180px]">
            <SelectValue placeholder="Select rollup" />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              <SelectItem value="year">Year</SelectItem>
              <SelectItem value="quarter">Quarter</SelectItem>
              <SelectItem value="month">Month</SelectItem>
            </SelectGroup>
          </SelectContent>
        </Select>
      </div>

      {/* --- Bar Chart --- */}
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={aggregatedData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey={xKey} />
          <YAxis />
          <Tooltip
            formatter={(value: number) => [`₱${value.toLocaleString()}`, 'Total Revenue']}
            labelFormatter={(label) => `${filterKey.toUpperCase()}: ${label}`}
          />
          <Legend />
          <Bar dataKey="total_revenue" fill="#3b82f6" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
