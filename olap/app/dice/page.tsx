'use client';

import { useMemo } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';

// Example data
const rawData = [
  { City: "East Kobe", Category: "electronics", Year: 2025, Quarter: 2, total_revenue: 385477.2 },
  { City: "Parkerside", Category: "toys", Year: 2025, Quarter: 2, total_revenue: 213244.35 },
  { City: "East Kobe", Category: "toys", Year: 2025, Quarter: 2, total_revenue: 39411.55 },
];

// ✅ Filter only the selected quarter and year
const DICE_YEAR = 2025;
const DICE_QUARTER = 2;

export default function DiceReport() {
  // Group by City → each Category as its own bar
  const data = useMemo(() => {
    const map: Record<string, any> = {};
    rawData
      .filter(d => d.Year === DICE_YEAR && d.Quarter === DICE_QUARTER)
      .forEach(({ City, Category, total_revenue }) => {
        if (!map[City]) map[City] = { City };
        map[City][Category] = (map[City][Category] || 0) + total_revenue;
      });
    return Object.values(map);
  }, []);

  // Extract all unique categories
  const categories = Array.from(new Set(rawData.map(d => d.Category)));

  return (
    <div className="p-8 bg-gray-50 min-h-screen">
      <h1 className="text-3xl font-bold mb-6 text-center text-gray-800">
        Dice Report: Cities vs Categories (Q{DICE_QUARTER}, {DICE_YEAR})
      </h1>

      <div className="w-full h-[450px] bg-white rounded-2xl shadow-md p-4">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 20, right: 30, left: 20, bottom: 10 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="City" />
            <YAxis
              label={{ value: 'Revenue', angle: -90, position: 'insideLeft' }}
              tickFormatter={(v) => v.toLocaleString()}
            />
            <Tooltip formatter={(v) => v.toLocaleString(undefined, { maximumFractionDigits: 2 })} />
            <Legend />

            {/* Generate bars per category dynamically */}
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
