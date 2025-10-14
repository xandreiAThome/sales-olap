import { fetcher } from '@/utils/fetcher';
import React, { useState } from 'react'
import { ResponsiveContainer, PieChart, Pie, Cell, Tooltip, Legend } from 'recharts';
import useSWR from 'swr';
import { Button } from './ui/button';
import { Input } from './ui/input';

const SliceDiv = () => {

    const [city, setCity] = useState('East Kobe');
    const [fetchKey, setFetchKey] = useState<string | null>(null); // 🔹 only fetch when this changes

    const { data: sliceData, error: sliceError, isLoading } = useSWR(fetchKey, fetcher);

    const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444'];

    const handleGenerate = () => {
        if (!city.trim()) return;
        setFetchKey(`http://localhost:4000/api/slice/${encodeURIComponent(city.trim())}`);
    };

  return (
    <div>
        <div className="flex flex-col items-center bg-gray-50 p-8 rounded-2xl shadow-md">
            <h1 className="text-3xl font-bold mb-6 text-gray-800 text-center">
            Slice Report - Product Breakdown
            </h1>
            
            {/* Input + Button */}
            <div className='flex gap-4'>
            <Input
                value={city}
                onChange={(e) => setCity(e.target.value)}
                placeholder="Enter City"
                className="mb-4 w-64"
            />
            <Button onClick={handleGenerate} className="mb-8 w-auto" disabled={!city.trim()}>
                Generate Report
            </Button>
            </div>

            {/* Conditional UI */}
            {!fetchKey ? (
            <div className="text-gray-600 mt-8">Please enter a city and click “Generate Report”.</div>
            ) : sliceError ? (
            <div className="text-red-500 p-4 text-center">Failed to load data for {city} 😢</div>
            ) : isLoading ? (
            <div className="text-gray-500 p-4 text-center">Loading data...</div>
            ) : !sliceData || sliceData.length === 0 ? (
            <div className="text-gray-500 p-4 text-center">No data found for {city}.</div>
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
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                    </Pie>

                    <Tooltip
                    formatter={(value: number) =>
                        value.toLocaleString(undefined, { maximumFractionDigits: 2 })
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
  )
}

export default SliceDiv