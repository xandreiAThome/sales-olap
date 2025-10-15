'use client';

import { fetcher } from '@/utils/fetcher';
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectGroup,
  SelectItem,
} from '@/components/ui/select';
import React, { useState } from 'react';
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
} from 'recharts';
import useSWR from 'swr';

const RollupDiv = () => {
  const { data: rollupData, error: rollupError } = useSWR(
    'http://localhost:4000/api/rollup',
    fetcher
  );

  type FilterKey = 'Year' | 'Quarter' | 'Month';
  const [filterKey, setFilterKey] = useState<FilterKey>('Year');

  if (rollupError) return <div className="p-6 text-red-500 font-semibold">Failed to load data 😢</div>;
  if (!rollupData) return <div className="p-6 text-gray-500 italic">Loading rollup data...</div>;

  const grouped: Record<string, number> = {}; 
  const quarterMap: Record<string, number> = {};
  const yearTotals: Record<number, number> = {};

  rollupData.forEach((item: any) => {
    const year = item.Year ?? null;
    const quarter = item.Quarter ?? null;
    const monthRaw = item.Month ?? null;
    const rev = item.revenue ?? 0;

    const month = monthRaw != null ? (Number.isFinite(Number(monthRaw)) ? parseInt(String(monthRaw), 10) : monthRaw) : null;

    if (filterKey === 'Year') {
      if (year == null && quarter == null && month == null) {
        grouped['Total Revenue'] = (grouped['Total Revenue'] || 0) + rev;
      } else if (year != null) {
        grouped[`${year}`] = (grouped[`${year}`] || 0) + rev;
      } else {
        grouped['Unknown Year'] = (grouped['Unknown Year'] || 0) + rev;
      }
    }

    else if (filterKey === 'Quarter') {
      if (year == null && quarter == null && month == null) return;

      if (quarter != null && year != null) {
        const qk = `Q${quarter}-${year}`;
        quarterMap[qk] = (quarterMap[qk] || 0) + rev;
        yearTotals[year] = (yearTotals[year] || 0) + rev;
      } else if (quarter != null) {
        const qk = `Q${quarter}`;
        quarterMap[qk] = (quarterMap[qk] || 0) + rev;
      } else if (year != null) {
        yearTotals[year] = (yearTotals[year] || 0) + rev;
      } else if (month != null) {
        const m = Number(month);
        if (Number.isFinite(m)) {
          const derivedQ = Math.ceil(m / 3);
          const qk = `Q${derivedQ}`;
          quarterMap[qk] = (quarterMap[qk] || 0) + rev;
        } else {
          quarterMap['Unknown'] = (quarterMap['Unknown'] || 0) + rev;
        }
      }
    }

    else if (filterKey === 'Month') {
      if (month == null) {
        return;
      }

      if (year != null) {
        grouped[`M${month}-${year}`] = (grouped[`M${month}-${year}`] || 0) + rev;
      } else {
        grouped[`M${month}`] = (grouped[`M${month}`] || 0) + rev;
      }
    }
  });

  const totalAll = rollupData.reduce((s: number, it: any) => s + (it.revenue ?? 0), 0);

  let aggregatedData: { label: string; revenue: number }[] = [];

  if (filterKey === 'Quarter') {
    const quarterEntries = Object.entries(quarterMap)
      .map(([k, v]) => {
        const m = k.match(/^Q(\d+)-(\d{4})$/);
        if (m) return { label: k, revenue: v, sortVal: parseInt(m[2], 10) * 10 + parseInt(m[1], 10) };
        const m2 = k.match(/^Q(\d+)$/);
        if (m2) return { label: k, revenue: v, sortVal: 9999 + parseInt(m2[1], 10) };
        if (k === 'Unknown') return { label: k, revenue: v, sortVal: 10000 };
        return { label: k, revenue: v, sortVal: 20000 };
      })
      .sort((a, b) => a.sortVal - b.sortVal)
      .map(({ label, revenue }) => ({ label, revenue }));

    aggregatedData.push(...quarterEntries);

    const yearEntries = Object.entries(yearTotals)
      .map(([yr, v]) => ({ label: `${yr}`, revenue: v, year: parseInt(yr, 10) }))
      .sort((a, b) => a.year - b.year)
      .map(({ label, revenue }) => ({ label, revenue }));

    aggregatedData.push(...yearEntries);
  } else {
    aggregatedData = Object.entries(grouped).map(([label, revenue]) => ({ label, revenue }));

    if (filterKey === 'Year') {
      const idx = aggregatedData.findIndex((d) => d.label === 'Total Revenue');
      if (idx >= 0) aggregatedData[idx].revenue = totalAll;
      else aggregatedData.unshift({ label: 'Total Revenue', revenue: totalAll });

      aggregatedData = aggregatedData.sort((a, b) => {
        if (a.label === 'Total Revenue') return -1;
        if (b.label === 'Total Revenue') return 1;
        const ay = a.label.match(/^(\d{4})$/);
        const by = b.label.match(/^(\d{4})$/);
        if (ay && by) return parseInt(ay[1], 10) - parseInt(by[1], 10);
        if (ay) return -1;
        if (by) return 1;
        return a.label.localeCompare(b.label);
      });
    } else {
      aggregatedData = aggregatedData
        .map((d) => ({ ...d }))
        .sort((a, b) => {
          const am = a.label.match(/^M(\d+)-(\d{4})$/);
          const bm = b.label.match(/^M(\d+)-(\d{4})$/);
          if (am && bm) {
            const ay = parseInt(am[2], 10), amn = parseInt(am[1], 10);
            const by = parseInt(bm[2], 10), bmn = parseInt(bm[1], 10);
            return ay === by ? amn - bmn : ay - by;
          }
          const aNoYear = a.label.match(/^M(\d+)$/);
          const bNoYear = b.label.match(/^M(\d+)$/);
          if (am && bNoYear) return -1;
          if (aNoYear && bm) return 1;
          if (aNoYear && bNoYear) return parseInt(aNoYear[1], 10) - parseInt(bNoYear[1], 10);
          return a.label.localeCompare(b.label);
        });
    }
  }

  if (aggregatedData.length === 0) return <div className="p-6 text-gray-500 italic">No data available.</div>;

  return (
    <div className="bg-white p-6 rounded-2xl shadow w-full max-w-auto mb-12">
      <h2 className="text-2xl font-bold mb-6">📊 Rollup Report - Sales Overview</h2>

      <div className="flex flex-wrap gap-4 mb-6">
        <Select value={filterKey} onValueChange={(val: FilterKey) => setFilterKey(val)}>
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
        <BarChart data={aggregatedData} margin={{ top: 20, right: 40, left: 80, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="label" />
          <YAxis
            tickFormatter={(v) => `${(v / 1e9).toFixed(2)}B`}
            label={{ value: 'Revenue (Billion ₱)', angle: -90, position: 'insideLeft', offset: 10 }}
          />
          <Tooltip formatter={(value: number) => `₱${(value / 1e9).toFixed(2)}B`} labelFormatter={(label) => `${filterKey}: ${label}`} />
          <Legend />
          <Bar dataKey="revenue" name="Total Revenue" fill="#3b82f6" />

          <Line
            type="monotone"
            dataKey="revenue"
            stroke="#ef4444"
            strokeWidth={3}
            dot={{ r: 4, stroke: '#ef4444', strokeWidth: 2 }}
            activeDot={{ r: 6 }}
            name="Revenue Trend"
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

export default RollupDiv;
