import { fetcher } from "@/utils/fetcher";
import React, { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import useSWR from "swr";
import { API_BASE_URL } from "@/lib/config";

const useProcessedData = (data: any[]) => {
  return useMemo(() => {
    if (!Array.isArray(data)) return [];

    const map: Record<string, any> = {};

    data.forEach(({ Courier_Name, Vehicle_Type, total_revenue }) => {
      if (!map[Courier_Name]) map[Courier_Name] = { Courier_Name };
      map[Courier_Name][Vehicle_Type] =
        (map[Courier_Name][Vehicle_Type] || 0) + total_revenue;
    });

    return Object.values(map);
  }, [data]);
};

const DrillDownDiv = () => {
  const { data: drillDownData, error: drillDownError } = useSWR(
    `${API_BASE_URL}/api/drillDown/`,
    fetcher
  );

  // viewMode: 'All' = total per courier, 'Breakdown' = vehicle types per courier (grouped bars)
  const [viewMode, setViewMode] = useState<"All" | "Breakdown">("All");

  const data = useProcessedData(drillDownData || []);

  const vehicleTypes = useMemo(() => {
    if (!Array.isArray(drillDownData)) return [];
    return Array.from(new Set(drillDownData.map((d) => d.Vehicle_Type)));
  }, [drillDownData]);

  // Prepare display data depending on viewMode
  const displayData = useMemo(() => {
    if (!Array.isArray(data)) return [];

    if (viewMode === "All") {
      // total per courier (sum of all vehicle types)
      return data.map((d) => {
        const total = vehicleTypes.reduce((sum, t) => sum + (d[t] || 0), 0);
        return {
          Courier_Name: d.Courier_Name,
          total,
        };
      });
    }

    // Breakdown mode: keep per-vehicle fields present in `data`
    return data;
  }, [data, viewMode, vehicleTypes]);

  if (!drillDownData) {
    return (
      <div className="p-6 text-gray-500 italic">Loading Drill Down data...</div>
    );
  }

  return (
    <div className="p-8 bg-gray-50 h-auto">
      <h1 className="text-3xl font-bold mb-6 text-gray-800 text-center">
        Courier Revenue — Total and Vehicle-Type Breakdown
      </h1>

      {/* Vehicle type selector */}
      <div className="flex justify-center mb-4">
        <label className="mr-2 self-center text-sm text-gray-700">Show:</label>
        <select
          value={viewMode}
          onChange={(e) => setViewMode(e.target.value as "All" | "Breakdown")}
          className="border rounded px-2 py-1"
        >
          <option value="All">Total per Courier (All Vehicles)</option>
          <option value="Breakdown">Vehicle Types per Courier (Grouped)</option>
        </select>
      </div>

      <div className="w-full h-[500px] bg-white rounded-2xl shadow-md p-4">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={displayData}
            margin={{ top: 20, right: 40, left: 40, bottom: 10 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="Courier_Name" />
            <YAxis
              tickFormatter={(v) => `${(v / 1e9).toFixed(2)}B`}
              label={{
                value: "Revenue (Billion)",
                angle: -90,
                position: "insideLeft",
                offset: 10,
              }}
            />
            <Tooltip
              formatter={(value: number) =>
                `₱${value.toLocaleString(undefined, {
                  maximumFractionDigits: 2,
                })}`
              }
            />
            <Legend />

            {/* Bars depending on selection */}
            {viewMode === "All" ? (
              <Bar dataKey="total" fill="#82ca9d" name="Total Revenue" />
            ) : viewMode === "Breakdown" ? (
              // Render one Bar per vehicle type (grouped bars)
              vehicleTypes.map((t, i) => (
                <Bar
                  key={t}
                  dataKey={t}
                  fill={
                    [
                      "#82ca9d",
                      "#8884d8",
                      "#ffc658",
                      "#ff8042",
                      "#8dd1e1",
                      "#a4de6c",
                      "#d0ed57",
                      "#ffbb28",
                    ][i % 8]
                  }
                  name={t}
                />
              ))
            ) : null}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default DrillDownDiv;
