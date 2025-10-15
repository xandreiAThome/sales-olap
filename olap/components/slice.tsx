import { fetcher } from "@/utils/fetcher";
import React, { useState, useEffect } from "react";
import {
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
} from "recharts";
import useSWR from "swr";
import { Button } from "./ui/button";
import Combobox from "./combobox";
import { API_BASE_URL } from "@/lib/config";

const SliceDiv = () => {
  const [city, setCity] = useState("East Kobe");
  const [fetchKey, setFetchKey] = useState<string | null>(null); // 🔹 only fetch when this changes

  const {
    data: sliceData,
    error: sliceError,
    isLoading,
  } = useSWR(fetchKey, fetcher);

  // For city autocomplete with debounce
  const [cityQuery, setCityQuery] = useState("");
  const [debouncedCityQuery, setDebouncedCityQuery] = useState(cityQuery);

  useEffect(() => {
    const t = setTimeout(() => setDebouncedCityQuery(cityQuery), 300);
    return () => clearTimeout(t);
  }, [cityQuery]);
  const citiesUrl = debouncedCityQuery
    ? `${API_BASE_URL}/api/cities?q=${encodeURIComponent(debouncedCityQuery)}`
    : `${API_BASE_URL}/api/cities`;
  const { data: cityOptions, isLoading: citiesLoading } = useSWR<string[]>(
    citiesUrl,
    fetcher
  );

  // Expanded color palette for pie chart (24 colors)
  const COLORS = [
    "#3b82f6", // blue-500
    "#2563eb", // blue-600
    "#06b6d4", // cyan-500
    "#0891b2", // cyan-600
    "#0ea5a4", // teal-500
    "#14b8a6", // teal-400
    "#10b981", // green-500
    "#34d399", // green-400
    "#84cc16", // lime-400
    "#bef264", // lime-200
    "#f59e0b", // amber-500
    "#f97316", // orange-500
    "#fb923c", // orange-400
    "#ef4444", // red-500
    "#f43f5e", // rose-500
    "#db2777", // pink-600
    "#a78bfa", // violet-300
    "#8b5cf6", // violet-500
    "#6366f1", // indigo-500
    "#e879f9", // fuchsia-400
    "#fca5a5", // rose-300
    "#b45309", // amber-700
    "#334155", // slate-700
    "#0f766e", // emerald-600
  ];

  const handleGenerate = () => {
    if (!city.trim()) return;
    setFetchKey(`${API_BASE_URL}/api/slice/${encodeURIComponent(city.trim())}`);
  };

  return (
    <div>
      <div className="flex flex-col items-center bg-gray-50 p-8 rounded-2xl shadow-md">
        <h1 className="text-3xl font-bold mb-6 text-gray-800 text-center">
          Slice Report - Product Breakdown
        </h1>

        {/* Input + Button */}
        <div className="flex gap-4">
          <div>
            <Combobox
              label="City"
              labelPosition="bottom"
              options={cityOptions || []}
              value={city}
              onChange={(val: string) => setCity(val)}
              placeholder="Enter City"
              inputValue={cityQuery}
              onInputChange={(val: string) => setCityQuery(val)}
              loading={citiesLoading}
            />
          </div>
          <Button
            onClick={handleGenerate}
            className="mb-8 w-auto"
            disabled={!city.trim()}
          >
            Generate Report
          </Button>
        </div>

        {/* Conditional UI */}
        {!fetchKey ? (
          <div className="text-gray-600 mt-8">
            Please enter a city and click “Generate Report”.
          </div>
        ) : sliceError ? (
          <div className="text-red-500 p-4 text-center">
            Failed to load data for {city} 😢
          </div>
        ) : isLoading ? (
          <div className="text-gray-500 p-4 text-center">Loading data...</div>
        ) : !sliceData || sliceData.length === 0 ? (
          <div className="text-gray-500 p-4 text-center">
            No data found for "{city}". Try another city or check the data
            source.
          </div>
        ) : (
          <div className="bg-white p-6 rounded-2xl shadow w-full max-w-auto mb-12">
            <h2 className="text-2xl font-bold mb-4">
              📊 Slice Report - Product Breakdown ({city})
            </h2>

            <ResponsiveContainer width="100%" height={650}>
              <PieChart>
                <Pie
                  data={sliceData}
                  cx="50%"
                  cy="50%"
                  outerRadius={120}
                  dataKey="total_revenue"
                  nameKey="Name"
                  labelLine={false}
                >
                  {sliceData.map((_: any, index: number) => (
                    <Cell
                      key={`cell-${index}`}
                      fill={COLORS[index % COLORS.length]}
                    />
                  ))}
                </Pie>

                <Tooltip
                  formatter={(value: number) =>
                    value.toLocaleString(undefined, {
                      maximumFractionDigits: 2,
                    })
                  }
                  labelFormatter={(name: string) => `Category: ${name}`}
                />

                <Legend
                  verticalAlign="bottom"
                  align="center"
                  iconType="circle"
                  layout="horizontal"
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </div>
  );
};

export default SliceDiv;
