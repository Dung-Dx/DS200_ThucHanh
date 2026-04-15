reviews = LOAD '/Lab2/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

negative_reviews = FILTER reviews BY sentiment == 'negative';

grouped_negative = GROUP negative_reviews BY aspect;

negative_count = FOREACH grouped_negative GENERATE
    group AS aspect,
    COUNT(negative_reviews) AS total_negative;

neg_all = GROUP negative_count ALL;
most_negative = FOREACH neg_all GENERATE
    FLATTEN(TOP(1, 1, negative_count));

positive_reviews = FILTER reviews BY sentiment == 'positive';

grouped_positive = GROUP positive_reviews BY aspect;

positive_count = FOREACH grouped_positive GENERATE
    group AS aspect,
    COUNT(positive_reviews) AS total_positive;

pos_all = GROUP positive_count ALL;
most_positive = FOREACH pos_all GENERATE
    FLATTEN(TOP(1, 1, positive_count));

STORE negative_count  INTO '/Lab2/output/Bai3_negative_count' USING PigStorage('\t');
STORE positive_count  INTO '/Lab2/output/Bai3_positive_count' USING PigStorage('\t');
STORE most_negative   INTO '/Lab2/output/Bai3_most_negative' USING PigStorage('\t');
STORE most_positive   INTO '/Lab2/output/Bai3_most_positive' USING PigStorage('\t');