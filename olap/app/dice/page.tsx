'use client';

import { useState, useMemo } from 'react';
import useSWR from 'swr';
import {
  Input
} from '@/components/ui/input';
import {
  Button
} from '@/components/ui/button'; // ✅ import Button properly
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { API_BASE_URL } from '@/lib/config';

const DICE_YEAR = 2025;
const DICE_QUARTER = 2;

const fetcher = async (url: string) => {
  const res = await fetch(url);
  if (!res.ok) throw new Error('Failed to fetch');
  return res.json();
};

export default function DiceReport() {
  const [city1, setCity1] = useState('');
  const [city2, setCity2] = useState('');
  const [fetchKey, setFetchKey] = useState<string | null>(null);

  const { data: rollupData, error: rollupError, isLoading } = useSWR(
    fetchKey,
    fetcher
  );

  const handleGenerate = () => {
    if (!city1 || !city2) return;
    const url = `${API_BASE_URL}/api/dice/${encodeURIComponent(city1)}/${encodeURIComponent(city2)}`;
    setFetchKey(url); // ✅ triggers SWR fetch
  };

  const data = useMemo(() => {
    if (!rollupData || !Array.isArray(rollupData)) return [];

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
    if (!rollupData || !Array.isArray(rollupData)) return [];
    return Array.from(new Set(rollupData.map((d: any) => d.Category)));
  }, [rollupData]);

  return (
    <div className="p-8 bg-gray-50 min-h-screen">
      <h1 className="text-3xl font-bold mb-6 text-center text-gray-800">
        Dice Report: Cities vs Categories (Q{DICE_QUARTER}, {DICE_YEAR})
      </h1>

      {/* City Inputs */}
      <div className="flex gap-4 mb-6 justify-center">
        <Input
          value={city1}
          onChange={(e) => setCity1(e.target.value)}
          placeholder="Enter City 1"
          className="w-[180px]"
        />
        <Input
          value={city2}
          onChange={(e) => setCity2(e.target.value)}
          placeholder="Enter City 2"
          className="w-[180px]"
        />
        <Button onClick={handleGenerate} disabled={!city1 || !city2}>
          Generate Report
        </Button>
      </div>

      {/* Chart or message */}
      {!fetchKey ? (
        <div className="text-gray-600 text-center mt-10">
          Please enter two cities and click "Generate Report".
        </div>
      ) : rollupError ? (
        <div className="text-red-500 p-4 text-center">
          Failed to load data for {city1} and {city2}.
        </div>
      ) : isLoading ? (
        <div className="text-gray-500 p-4 text-center">Loading data...</div>
      ) : (
        <div className="w-full h-[450px] bg-white rounded-2xl shadow-md p-4">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} margin={{ top: 20, right: 30, left: 40, bottom: 10 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="City" />
              <YAxis
                label={{ value: 'Revenue', angle: -90, position: 'insideLeft' }}
                tickFormatter={(v) => v.toLocaleString()}
              />
              <Tooltip
                formatter={(v) =>
                  Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 })
                }
              />
              <Legend />
              {categories.map((cat: any, i: number) => (
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
      )}
    </div>
  );
}
