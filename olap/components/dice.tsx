'use client';

import { fetcher } from '@/utils/fetcher';
import React, { useMemo, useState, useEffect } from 'react';
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
import { Input } from './ui/input';
import { API_BASE_URL } from '@/lib/config';

const DICE_YEAR = 2025;
const DICE_QUARTER = 2;

const DiceDiv = () => {
  const [city1, setCity1] = useState('');
  const [city2, setCity2] = useState('');
  const [category1, setCategory1] = useState('');
  const [category2, setCategory2] = useState('');

  const [diceData, setDiceData] = useState<any[] | null>(null);
  const [diceError, setDiceError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  // Fetch categories via SWR
  const {
    data: categoriesData,
    error: categoriesError,
    isLoading: categoriesLoading,
  } = useSWR(`${API_BASE_URL}/api/categories`, fetcher);

  // Always default to empty array
  const categories: string[] = Array.isArray(categoriesData)
    ? categoriesData
    : [];

  // Reset chart when filters change
  useEffect(() => {
    setDiceData(null);
    setDiceError(null);
  }, [city1, city2, category1, category2]);

  const handleGenerate = async () => {
    if (!city1 || !city2 || !category1 || !category2) return;

    const url = `${API_BASE_URL}/api/dice/${encodeURIComponent(
      city1
    )}/${encodeURIComponent(city2)}/${encodeURIComponent(
      category1
    )}/${encodeURIComponent(category2)}`;

    setIsLoading(true);
    setDiceError(null);
    setDiceData(null);

    try {
      const result = await fetcher(url);
      if (!result || !Array.isArray(result)) {
        setDiceData([]);
      } else {
        setDiceData(result);
      }
    } catch (err: any) {
      setDiceError(err?.message || 'Failed to fetch dice data');
      setDiceData([]);
    } finally {
      setIsLoading(false);
    }
  };

  const chartData = useMemo(() => {
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

  return (
    <div className="flex flex-col items-center bg-gray-50 p-8 rounded-2xl shadow-md">
      <div className="p-8 bg-gray-50 min-h-screen">
        <h1 className="text-3xl font-bold mb-6 text-center text-gray-800">
          Dice Report: Cities vs Categories (Q{DICE_QUARTER}, {DICE_YEAR})
        </h1>

        {/* City and Category Selectors */}
        <div className="flex flex-wrap gap-4 mb-6 justify-center items-end">
          {/* City 1 */}
          <div className="flex flex-col items-start">
            <label className="text-sm font-medium mb-1">City 1</label>
            <Input
              className="w-[200px]"
              value={city1}
              onChange={(e) => setCity1(e.target.value)}
              placeholder="Enter City 1"
            />
          </div>

          {/* City 2 */}
          <div className="flex flex-col items-start">
            <label className="text-sm font-medium mb-1">City 2</label>
            <Input
              className="w-[200px]"
              value={city2}
              onChange={(e) => setCity2(e.target.value)}
              placeholder="Enter City 2"
            />
          </div>

          {/* Categories combobox */}
          {categoriesLoading ? (
            <div className="text-gray-500 text-center mt-2">
              Loading categories...
            </div>
          ) : categoriesError ? (
            <div className="text-red-500 text-center mt-2">
              Failed to load categories.
            </div>
          ) : categories.length > 0 ? (
            <>
              <Combobox
                label="Category 1"
                options={categories}
                value={category1}
                onChange={setCategory1}
                placeholder="Select Category 1"
              />
              <Combobox
                label="Category 2"
                options={categories}
                value={category2}
                onChange={setCategory2}
                placeholder="Select Category 2"
              />
            </>
          ) : (
            <div className="text-gray-500 text-center mt-2">
              No categories found.
            </div>
          )}

          {/* Generate Button */}
          <div className="flex items-end">
            <Button
              onClick={handleGenerate}
              disabled={
                !city1 ||
                !city2 ||
                !category1 ||
                !category2 ||
                isLoading ||
                categoriesLoading
              }
            >
              {isLoading ? 'Generating...' : 'Generate Report'}
            </Button>
          </div>
        </div>

        {/* Chart or message */}
        {diceError ? (
          <div className="text-red-500 p-4 text-center">
            Failed to load data: {diceError}
          </div>
        ) : isLoading ? (
          <div className="text-gray-500 p-4 text-center">Loading data...</div>
        ) : !diceData ? (
          <div className="text-gray-600 text-center mt-10">
            Please select two cities and two categories, then click "Generate
            Report".
          </div>
        ) : diceData.length === 0 ? (
          <div className="text-gray-600 text-center mt-10">
            No results found for your selected filters.
          </div>
        ) : (
          <div className="w-full h-[450px] bg-white rounded-2xl shadow-md p-4">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={chartData}
                margin={{ top: 20, right: 30, left: 40, bottom: 10 }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="City" />
                <YAxis
                  label={{ value: 'Revenue', angle: -90, position: 'insideLeft' }}
                  tickFormatter={(v) =>
                    typeof v === 'number' ? v.toLocaleString() : v
                  }
                />
                <Tooltip
                  formatter={(v) =>
                    Number(v).toLocaleString(undefined, {
                      maximumFractionDigits: 2,
                    })
                  }
                />
                <Legend />
                {[category1, category2].map((cat, i) => (
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
  </div>
  );
};

export default DiceDiv;
