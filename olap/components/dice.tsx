'use client';

import { fetcher } from '@/utils/fetcher';
import React, { useMemo, useState } from 'react';
import {
  ResponsiveContainer,
  BarChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Bar,
} from 'recharts';
import useSWR from 'swr';
import { Button } from './ui/button';
import Combobox from './combobox';

interface DiceData {
  cities: string[];
}

const DICE_YEAR = 2025;
const DICE_QUARTER = 2;

const DiceDiv = ({ cities }: DiceData) => {
  const [city1, setCity1] = useState('');
  const [city2, setCity2] = useState('');
  const [category1, setCategory1] = useState('');
  const [category2, setCategory2] = useState('');
  const [fetchKey, setFetchKey] = useState<string | null>(null);

  const { data: diceData, error: diceError, isLoading } = useSWR(
    fetchKey,
    fetcher
  );

  const {data: categoriesData, error: categoriesError} = useSWR(
    'http://localhost:4000/api/categories',
    fetcher
  );

  const handleGenerate = () => {
    if (!city1 || !city2 || !category1 || !category2) return;
    const url = `http://localhost:4000/api/dice/${encodeURIComponent(
      city1
    )}/${encodeURIComponent(city2)}/${encodeURIComponent(
      category1
    )}/${encodeURIComponent(category2)}`;
    setFetchKey(url);
  };

  const data = useMemo(() => {
    if (!diceData || !Array.isArray(diceData)) return [];

    const map: Record<string, any> = {};

    diceData
      .filter((d: any) => d.Year === DICE_YEAR && d.Quarter === DICE_QUARTER)
      .forEach(({ City, Category, total_revenue }: any) => {
        if (!map[City]) map[City] = { City };
        map[City][Category] = (map[City][Category] || 0) + total_revenue;
      });

    return Object.values(map);
  }, [diceData]);

  // Dynamically extract categories after fetching
  const categories = useMemo(() => {
    if (!diceData || !Array.isArray(diceData)) return [];
    return Array.from(new Set(diceData.map((d: any) => d.Category)));
  }, [diceData]);

  return (
    <div className="p-8 bg-gray-50 h-auto">
      <h1 className="text-3xl font-bold mb-6 text-center text-gray-800">
        Dice Report: Cities vs Categories (Q{DICE_QUARTER}, {DICE_YEAR})
      </h1>

      {/* City and Category Selectors */}
      <div className="flex flex-wrap gap-4 mb-6 justify-center">
        <Combobox
          label="City 1"
          options={cities}
          value={city1}
          onChange={setCity1}
          placeholder="Select City 1"
        />
        <Combobox
          label="City 2"
          options={cities}
          value={city2}
          onChange={setCity2}
          placeholder="Select City 2"
        />
        <Combobox
          label="Category 1"
          options={categories.length ? categories : ['electronics', 'toys', 'clothing']}
          value={category1}
          onChange={setCategory1}
          placeholder="Select Category 1"
        />
        <Combobox
          label="Category 2"
          options={categories.length ? categories : ['electronics', 'toys', 'clothing']}
          value={category2}
          onChange={setCategory2}
          placeholder="Select Category 2"
        />

        <div className="flex items-end">
          <Button
            onClick={handleGenerate}
            disabled={!city1 || !city2 || !category1 || !category2}
          >
            Generate Report
          </Button>
        </div>
      </div>

      {/* Chart or message */}
      {!fetchKey ? (
        <div className="text-gray-600 text-center mt-10">
          Please select two cities and two categories, then click "Generate Report".
        </div>
      ) : diceError ? (
        <div className="text-red-500 p-4 text-center">
          Failed to load data for {city1} and {city2}.
        </div>
      ) : isLoading ? (
        <div className="text-gray-500 p-4 text-center">Loading data...</div>
      ) : (
        <div className="w-full h-[450px] bg-white rounded-2xl shadow-md p-4">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={data}
              margin={{ top: 20, right: 30, left: 40, bottom: 10 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="City" />
              <YAxis
                label={{ value: 'Revenue', angle: -90, position: 'insideLeft' }}
                tickFormatter={(v) => v.toLocaleString()}
              />
              <Tooltip
                formatter={(v) =>
                  Number(v).toLocaleString(undefined, {
                    maximumFractionDigits: 2,
                  })
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
};

export default DiceDiv;
