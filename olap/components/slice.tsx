import { fetcher } from "@/utils/fetcher";
import React, { useState, useEffect, useMemo } from "react";
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
  const [fetchKey, setFetchKey] = useState<string | null>(null);

  const { data: sliceData, error: sliceError, isLoading } = useSWR(
    fetchKey,
    fetcher
  );

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

  const COLORS = [
    "#3b82f6", "#2563eb", "#06b6d4", "#0891b2", "#0ea5a4", "#14b8a6",
    "#10b981", "#34d399", "#84cc16", "#bef264", "#f59e0b", "#f97316",
    "#fb923c", "#ef4444", "#f43f5e", "#db2777", "#a78bfa", "#8b5cf6",
    "#6366f1", "#e879f9", "#fca5a5", "#b45309", "#334155", "#0f766e",
  ];

  const handleGenerate = () => {
    if (!city.trim()) return;
    setFetchKey(`${API_BASE_URL}/api/slice/${encodeURIComponent(city.trim())}`);
  };

  const TOP_N = 50; // Show only top N categories (baguhin nlng depends sa needs)
  const LEGEND_MAX = 200; // Still show legend if <= N (baguhin nlng depends sa needs)

  const processedData = useMemo(() => {
    if (!sliceData || !Array.isArray(sliceData)) return [];

    const cleaned = sliceData.map((d: any) => ({
      ...d,
      total_revenue: Number(d.total_revenue ?? 0),
      Name: String(d.Name ?? "Unknown"),
    }));

    return cleaned.sort((a: any, b: any) => b.total_revenue - a.total_revenue).slice(0, TOP_N);
  }, [sliceData]);

  const showLegend = processedData.length <= LEGEND_MAX;

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

        {/* --- CONDITIONAL RENDERING --- */}
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
            No data found for "{city}". Try another city or check the data source.
          </div>
        ) : (
          <div className="bg-white p-6 rounded-2xl shadow w-full max-w-auto mb-12">
            <h2 className="text-2xl font-bold mb-4">
              📊 Slice Report - Product Breakdown ({city})
            </h2>

            <ResponsiveContainer width="100%" height={650}>
              <PieChart>
                <Pie
                  data={processedData}
                  cx="50%"
                  cy="50%"
                  outerRadius={120}
                  dataKey="total_revenue"
                  nameKey="Name"
                  labelLine={false}
                >
                  {processedData.map((_: any, index: number) => (
                    <Cell
                      key={`cell-${index}`}
                      fill={COLORS[index % COLORS.length]}
                    />
                  ))}
                </Pie>

                <Tooltip
                  formatter={(value: number) =>
                    value.toLocaleString(undefined, { maximumFractionDigits: 2 })
                  }
                  labelFormatter={(name: string) => `Category: ${name}`}
                />

                {showLegend && (
                  <Legend
                    verticalAlign="bottom"
                    align="center"
                    iconType="circle"
                    layout="horizontal"
                  />
                )}
              </PieChart>
            </ResponsiveContainer>

            {!showLegend && (
              <div className="text-sm text-gray-500 mt-2">
                Legend hidden because there are many items — hover slices to see details.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default SliceDiv;
