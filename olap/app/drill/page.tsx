'use client';

import { useMemo } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts';

// Example data (paste yours here)
const rawData = [
  { Courier_Name: "JNT", Vehicle_Type: "motorcycle", First_Name: "Jaqueline", Last_Name: "Toy", total_revenue: 2842123403.95 },
  { Courier_Name: "LBCD", Vehicle_Type: "bicycle", First_Name: "Albin", Last_Name: "Krajcik", total_revenue: 2822534257.45 },
  { Courier_Name: "FEDEZ", Vehicle_Type: "bike", First_Name: "Curtis", Last_Name: "Gutkowski", total_revenue: 2818598317.1 },
  { Courier_Name: "FEDEZ", Vehicle_Type: "motorcycle", First_Name: "Nayeli", Last_Name: "Ritchie", total_revenue: 2812948359.85 },
  { Courier_Name: "LBCD", Vehicle_Type: "bicycle", First_Name: "Carmine", Last_Name: "Bogan", total_revenue: 2798527182.7 },
  { Courier_Name: "JNT", Vehicle_Type: "motorbike", First_Name: "Timothy", Last_Name: "Grimes", total_revenue: 2793212253.65 },
];

// ✅ Transform raw data → group by courier and vehicle type
const useProcessedData = (data: typeof rawData) => {
  return useMemo(() => {
    const map: Record<string, any> = {};
    data.forEach(({ Courier_Name, Vehicle_Type, total_revenue }) => {
      if (!map[Courier_Name]) map[Courier_Name] = { Courier_Name };
      map[Courier_Name][Vehicle_Type] = (map[Courier_Name][Vehicle_Type] || 0) + total_revenue;
    });
    return Object.values(map);
  }, [data]);
};

export default function CourierRevenueChart() {
  const data = useProcessedData(rawData);

  return (
    <div className="p-8 bg-gray-50 min-h-screen">
      <h1 className="text-3xl font-bold mb-6 text-gray-800 text-center">
        Courier Revenue by Vehicle Type
      </h1>

      <div className="w-full h-[500px] bg-white rounded-2xl shadow-md p-4">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 20, right: 30, left: 20, bottom: 10 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="Courier_Name" />
            <YAxis
              tickFormatter={(v) => `${(v / 1e9).toFixed(2)}B`}
              label={{ value: 'Revenue (Billion)', angle: -90, position: 'insideLeft' }}
            />
            <Tooltip
              formatter={(value: number) => value.toLocaleString(undefined, { maximumFractionDigits: 2 })}
            />
            <Legend />
            {/* Dynamic bars based on vehicle types */}
            {Array.from(
              new Set(rawData.map((d) => d.Vehicle_Type))
            ).map((type, i) => (
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
  );
}
