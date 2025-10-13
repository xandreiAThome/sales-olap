'use client';

import { useMemo } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';
import useSWR from 'swr';

const DICE_YEAR = 2025;
const DICE_QUARTER = 2;

const fetcher = async (url: string) => {
  const res = await fetch(url);
  if (!res.ok) throw new Error('Failed to fetch');
  return res.json();
};

export default function DiceReport() {
  const { data: rollupData, error: rollupError } = useSWR(
    'http://localhost:4000/api/dice/East Kobe/Parkerside',
    fetcher
  );

  const data = useMemo(() => {
    if (!rollupData) return [];

    const map: Record<string, any> = {};

    rollupData
      .filter((d: any) => d.Year === DICE_YEAR && d.Quarter === DICE_QUARTER)
      .forEach(({ City, Category, total_revenue }: any) => {
        if (!map[City]) map[City] = { City };
        map[City][Category] = (map[City][Category] || 0) + total_revenue;
      });

    return Object.values(map);
  }, [rollupData]);

  const categories = useMemo(() => {
    if (!rollupData) return [];
    return Array.from(new Set(rollupData.map((d: any) => d.Category)));
  }, [rollupData]);

  if (rollupError) return <div className="text-red-500 p-4">Failed to load data</div>;
  if (!rollupData) return <div className="text-gray-500 p-4">Loading...</div>;

  return (
    <div className="p-8 bg-gray-50 min-h-screen">
      <h1 className="text-3xl font-bold mb-6 text-center text-gray-800">
        Dice Report: Cities vs Categories (Q{DICE_QUARTER}, {DICE_YEAR})
      </h1>

      <div className="w-full h-[450px] bg-white rounded-2xl shadow-md p-4">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={data}
            margin={{ top: 20, right: 30, left: 40, bottom: 10 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="City" />
            <YAxis
              label={{
                value: 'Revenue',
                angle: -90,
                position: 'insideLeft',
              }}
              tickFormatter={(v) => v.toLocaleString()}
            />
            <Tooltip
              formatter={(v) =>
                v.toLocaleString(undefined, { maximumFractionDigits: 2 })
              }
            />
            <Legend />

            {categories.map((cat, i) => (
              <Bar
                key={cat}
                dataKey={cat}
                fill={['#82ca9d', '#8884d8', '#ffc658', '#ff8042'][i % 4]}
                name={cat.charAt(0).toUpperCase() + cat.slice(1)}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
