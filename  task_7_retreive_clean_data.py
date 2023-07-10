def clean_orders_data(self, data):
        """Cleans the orders data.

        Args:
            data: The DataFrame of the orders data.

        Returns:
            A DataFrame of the orders data after cleaning.
        """

        # Drop the columns first_name, last_name and 1.
        data.drop(['first_name', 'last_name', 1], axis=1, inplace=True)

        # Check for NULL values.
        for row in data.iterrows():
            for column in data.columns:
                if row[1][column] is None:
                    row[1][column] = ''

        # Check for errors with dates.
        for row in data.iterrows():
            if 'order_date' in data:
                try:
                    datetime.datetime.strptime(row[1]['order_date'], '%Y-%m-%d')
                except ValueError:
                    row[1]['order_date'] = ''

        # Check for incorrectly typed values.
        for row in data.iterrows():
            for column in data.columns:
                try:
                    int(row[1][column])
                except ValueError:
                    try:
                        float(row[1][column])
                    except ValueError:
                        row[1][column] = ''

        # Check for rows filled with the wrong information.
        for row in data.iterrows():
            if len(row[1]) == 0:
                data.drop(row[0], inplace=True)

        return data
