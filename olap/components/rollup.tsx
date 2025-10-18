import { fetcher } from "@/utils/fetcher";
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectGroup,
  SelectItem,
} from "@/components/ui/select";
import React, { useMemo, useState } from "react";
import {
  BarChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Bar,
  ResponsiveContainer,
  Line,
} from "recharts";
import useSWR from "swr";
import { API_BASE_URL } from "@/lib/config";

const RollupDiv = () => {
  const { data: rollupData, error: rollupError } = useSWR(
    `${API_BASE_URL}/api/rollup`,
    fetcher
  );

  type FilterKey = "Year" | "Quarter" | "Month";
  const [filterKey, setFilterKey] = useState<FilterKey>("Year");

  // 🧠 Prepare chart data based on the selected filter
  const aggregatedData = useMemo(() => {
    if (!rollupData || !Array.isArray(rollupData)) return [];

    const cleaned = rollupData
      .map((d: any) => ({
        ...d,
        revenue: Number(d.revenue ?? 0),
        Year: d.Year ?? null,
        Quarter: d.Quarter ?? null,
        Month: d.Month ?? null,
      }))
      .filter((d: any) => d.revenue > 0);

    let labeled: { label: string; revenue: number }[] = [];

    if (filterKey === "Year") {
      labeled = cleaned
        .filter((d: any) => d.Year != null && d.Quarter == null && d.Month == null)
        .map((d: any) => ({
          label: `${d.Year}`,
          revenue: d.revenue,
        }));

      const grandTotal = cleaned.find(
        (d: any) => d.Year === null && d.Quarter === null && d.Month === null
      );
      if (grandTotal) {
        labeled.push({
          label: "Grand Total",
          revenue: grandTotal.revenue,
        });
      }
    }

    else if (filterKey === "Quarter") {
      labeled = cleaned
        .filter((d: any) => d.Year != null && d.Quarter != null && d.Month == null)
        .map((d: any) => ({
          label: `Q${d.Quarter}-${d.Year}`,
          revenue: d.revenue,
        }));
    }

    else if (filterKey === "Month") {
      labeled = cleaned
        .filter((d: any) => d.Year != null && d.Month != null)
        .map((d: any) => ({
          label: `M${d.Month}-${d.Year}`,
          revenue: d.revenue,
        }));
    }

    labeled.sort((a: any, b: any) => {
      if (a.label === "Grand Total") return 1;
      if (b.label === "Grand Total") return -1;
      const [aPart, aYear] = a.label.split("-").map((x: string) => x.replace(/[A-Z]/g, ""));
      const [bPart, bYear] = b.label.split("-").map((x: string) => x.replace(/[A-Z]/g, ""));
      return Number(aYear) - Number(bYear) || Number(aPart) - Number(bPart);
    });

    return labeled;
  }, [rollupData, filterKey]);

  if (rollupError)
    return <div className="p-6 text-red-500">Failed to load rollup data 😢</div>;

  if (!aggregatedData || aggregatedData.length === 0)
    return <div className="p-6 text-gray-500 italic">No data available.</div>;

  return (
    <div className="bg-white p-6 rounded-2xl shadow w-full max-w-auto mb-12">
      <h2 className="text-2xl font-bold mb-6">
        📊 Rollup Report - Sales Trend Overview
      </h2>

      {/* --- Filter Selector --- */}
      <div className="flex flex-wrap gap-4 mb-6">
        <Select
          value={filterKey}
          onValueChange={(val: FilterKey) => setFilterKey(val)}
        >
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

      <ResponsiveContainer width="100%" height={600}>
        <BarChart
          data={aggregatedData}
          margin={{ top: 20, right: 40, left: 80, bottom: 20 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <Bar dataKey="revenue" name="Total Revenue" fill="#3b82f6" />
          <YAxis
            tickFormatter={(v) => `${(v / 1e9).toFixed(2)}B`}
            label={{
              value: "Revenue (Billion ₱)",
              angle: -90,
              position: "insideLeft",
              offset: 10,
            }}
          />
          <Tooltip
            formatter={(value: number) => `₱${(value / 1e9).toFixed(2)}B`}
            labelFormatter={(label) => `${filterKey}: ${label}`}
          />
          <Legend />
          <XAxis
            dataKey="label"
            interval={0}
            angle={-45}
            className="text-xs z-50"
          />
          {/* 🔴 Trend line */}
          <Line
            type="monotone"
            dataKey="revenue"
            stroke="#ef4444"
            strokeWidth={3}
            dot={{ r: 4, stroke: "#ef4444", strokeWidth: 2 }}
            activeDot={{ r: 6 }}
            name="Revenue Trend"
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

export default RollupDiv;
