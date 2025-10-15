import { fetcher } from '@/utils/fetcher';
import React, { useMemo } from 'react'
import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import useSWR from 'swr';


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
        'http://localhost:4000/api/drillDown/',
        fetcher
    );

    const data = useProcessedData(drillDownData || []);

    const vehicleTypes = useMemo(() => {
        if (!Array.isArray(drillDownData)) return [];
        return Array.from(new Set(drillDownData.map((d) => d.Vehicle_Type)));
    }, [drillDownData]);


  if (!drillDownData) {
    return <div className="p-6 text-gray-500 italic">Loading Drill Down data...</div>;
  }

  return (
    <div className="p-8 bg-gray-50 h-auto">
      <h1 className="text-3xl font-bold mb-6 text-gray-800 text-center">
        Courier Revenue by Vehicle Type
      </h1>

      <div className="w-full h-[500px] bg-white rounded-2xl shadow-md p-4">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={data}
            margin={{ top: 20, right: 40, left: 40, bottom: 10 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="Courier_Name" />
            <YAxis
              tickFormatter={(v) => `${(v / 1e9).toFixed(2)}B`}
              label={{
                value: 'Revenue (Billion)',
                angle: -90,
                position: 'insideLeft',
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

            {/* Dynamic bars per vehicle type */}
            {vehicleTypes.map((type, i) => (
              <Bar
                key={type}
                dataKey={type}
                fill={['#82ca9d', '#8884d8', '#ffc658', '#ff8042', '#8dd1e1'][i % 5]}
                name={type.charAt(0).toUpperCase() + type.slice(1)}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

export default DrillDownDiv