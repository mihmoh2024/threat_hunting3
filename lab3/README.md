# Лабораторная работа №3.Анализ данных сетевого трафика
Смирнов Михаил БИСО-03-20

## Цель работы

1\. Изучить возможности технологии Apache Arrow для обработки и анализ
больших данных 2. Получить навыки применения Arrow совместно с языком
программирования R 3. Получить навыки анализа метаинфомации о сетевом
трафике 4. Получить навыки применения облачных технологий хранения,
подготовки и анализа данных: Yandex Object Storage, Rstudio Server.

## Ход работы

``` r
library(arrow)
```

    Some features are not enabled in this build of Arrow. Run `arrow_info()` for more information.


    Attaching package: 'arrow'

    The following object is masked from 'package:utils':

        timestamp

``` r
library(dplyr)
```


    Attaching package: 'dplyr'

    The following objects are masked from 'package:stats':

        filter, lag

    The following objects are masked from 'package:base':

        intersect, setdiff, setequal, union

``` r
library(tidyverse)
```

    ── Attaching core tidyverse packages ──────────────────────── tidyverse 2.0.0 ──
    ✔ forcats   1.0.0     ✔ readr     2.1.5
    ✔ ggplot2   3.4.4     ✔ stringr   1.5.1
    ✔ lubridate 1.9.3     ✔ tibble    3.2.1
    ✔ purrr     1.0.2     ✔ tidyr     1.3.1

    ── Conflicts ────────────────────────────────────────── tidyverse_conflicts() ──
    ✖ lubridate::duration() masks arrow::duration()
    ✖ dplyr::filter()       masks stats::filter()
    ✖ dplyr::lag()          masks stats::lag()
    ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors

### Доступ к датасету

``` r
dir.create("data", showWarnings = FALSE)
```

``` r
curl::multi_download("https://storage.yandexcloud.net/arrow-datasets/tm_data.pqt", "data/tm_data.pqt",resume = TRUE)
```

    # A tibble: 1 × 10
      success status_code resumefrom url    destfile error type  modified           
      <lgl>         <int>      <dbl> <chr>  <chr>    <chr> <chr> <dttm>             
    1 TRUE            200          0 https… data/tm… <NA>  appl… 2024-02-19 12:25:05
    # ℹ 2 more variables: time <dbl>, headers <list>

### Чтение датасета

``` r
data <- read_parquet("data/tm_data.pqt", as_data_frame = FALSE)
```

``` r
print(data)
```

    Table
    105747730 rows x 5 columns
    $timestamp <double>
    $src <string>
    $dst <string>
    $port <int32>
    $bytes <int32>

    See $metadata for additional Schema metadata

### Задание 1: Надите утечку данных из Вашей сети

Важнейшие документы с результатами нашей исследовательской деятельности
в области создания вакцин скачиваются в виде больших заархивированных
дампов. Один из хостов в нашей сети используется для пересылки этой
информации – он пересылает гораздо больше информации на внешние ресурсы
в Интернете, чем остальные компьютеры нашей сети. Определите его
IP-адрес.

``` r
out <- out <- data %>% 
  select(src, dst, bytes) %>% 
  filter(!str_detect(dst, '1[2-4].*')) %>% 
  select(src, bytes) %>% 
  group_by(src) %>% 
  summarize(sum_bytes = sum(bytes)) %>% 
  filter(sum_bytes == max(sum_bytes))
```

    Warning: Expression sum_bytes == max(sum_bytes) not supported in Arrow; pulling
    data into R

``` r
out
```

    # A tibble: 1 × 2
      src           sum_bytes
      <chr>           <int64>
    1 13.37.84.125 5765792351

### Задание 2: Надите утечку данных 2

Другой атакующий установил автоматическую задачу в системном
планировщике cron для экспорта содержимого внутренней wiki системы. Эта
система генерирует большое количество трафика в нерабочие часы, больше
чем остальные хосты. Определите IP этой системы. Известно, что ее IP
адрес отличается от нарушителя из предыдущей задачи.

Для начала определим рабочие часы:

``` r
data_filter <- data %>% 
  select(timestamp, src, dst, bytes) %>% 
  mutate(trafic = (str_detect(src, '1[2-4].*') & 
                     !str_detect(dst, '1[2-4].*')),
         time = hour(as_datetime(timestamp/1000))) %>%
  filter(trafic == TRUE, time >= 0 & time <= 24) %>%
  group_by(time) %>% 
  summarise(trafictime = n()) %>% 
  arrange(desc(trafictime))
```

``` r
data_filter |> collect()
```

    # A tibble: 24 × 2
        time trafictime
       <int>      <int>
     1    18    3305646
     2    23    3305086
     3    16    3304767
     4    22    3304743
     5    19    3303518
     6    21    3303328
     7    17    3301627
     8    20    3300709
     9    13     124857
    10     0     124851
    # ℹ 14 more rows

``` r
data_last <- data %>% 
  mutate(time = hour(as_datetime(timestamp/1000))) %>%
  filter(!str_detect(src, "^13.37.84.125")) %>%
  filter(str_detect(src, '1[2-4].*')) %>%
  filter(!str_detect(dst, '1[2-4].*')) %>% 
  filter(time >= 1 & time <= 15) %>% 
  group_by(src) %>% summarise("sum" = sum(bytes)) %>%
  select(src,sum)
```

``` r
data_last |> collect()
```

    # A tibble: 999 × 2
       src                sum
       <chr>            <int>
     1 12.33.28.63   46319321
     2 12.54.40.45   63721890
     3 12.33.52.61   31928340
     4 14.33.97.28   30752007
     5 13.39.115.125 33555070
     6 14.45.90.51   47684508
     7 12.33.36.105  64538350
     8 12.34.55.109  52186905
     9 12.48.58.57   51044046
    10 13.35.63.115  34558696
    # ℹ 989 more rows

``` r
data_last_head1 <- data_last %>% 
  arrange(desc(sum)) %>% 
  head(1)
```

``` r
data_last_head1 |> collect()
```

    # A tibble: 1 × 2
      src               sum
      <chr>           <int>
    1 12.55.77.96 191826796

### Задание 3: Надите утечку данных 3

Еще один нарушитель собирает содержимое электронной почты и отправляет в
Интернет используя порт, который обычно используется для другого типа
трафика. Атакующий пересылает большое количество информации используя
этот порт, которое нехарактерно для других хостов, использующих этот
номер порта. Определите IP этой системы. Известно, что ее IP адрес
отличается от нарушителей из предыдущих задач.

Отбираем интересующие нас ip адреса

``` r
ipaddr1 <- data %>% 
  filter(!str_detect(src, "^13.37.84.125")) %>%
  filter(!str_detect(src, "^12.55.77.96")) %>%
  filter(str_detect(src, "^12.") 
         | str_detect(src, "^13.") 
         | str_detect(src, "^14."))  %>% 
  filter(!str_detect(dst, "^12.") 
         | !str_detect(dst, "^13.") 
         | !str_detect(dst, "^14."))  %>% 
  select(src, bytes, port)
```

``` r
ipaddr1 |> collect()
```

    # A tibble: 57,913,813 × 3
       src           bytes  port
     * <chr>         <int> <int>
     1 13.43.52.51   57354    40
     2 14.33.30.103  20979   115
     3 12.46.104.126  1500   123
     4 12.43.98.93     979    79
     5 14.32.60.107   1036    72
     6 13.48.126.55   1500   123
     7 14.51.37.21    1500   123
     8 14.49.44.92    1152    22
     9 14.53.76.24    5775   118
    10 14.37.108.54   1086    94
    # ℹ 57,913,803 more rows

группируем по портам? вычисляем максимальный и средний размер трафика и
находим порт с большей разницей

``` r
ipaddr2 <-ipaddr1 %>%  
  group_by(port) %>% 
  summarise("mean"=mean(bytes), 
            "max"=max(bytes), 
            "sum" = sum(bytes)) %>%  
  mutate("raznitsa"= max-mean) %>% 
  arrange(desc(raznitsa)) %>% 
  head(1)
```

``` r
ipaddr2 |> collect()
```

    # A tibble: 1 × 5
       port   mean    max         sum raznitsa
      <int>  <dbl>  <int>     <int64>    <dbl>
    1    37 33348. 209402 48192673159  176054.

Мы определили порт, который использовался для другого трафика

``` r
ipaddr3 <- ipaddr1  %>% 
  filter(port==37) %>% 
  group_by(src) %>% 
  summarise("mean"= mean(bytes)) %>%
  arrange(desc(mean)) %>% 
  head(1) %>%
  select(src)
```

``` r
ipaddr3 |> collect()
```

    # A tibble: 1 × 1
      src        
      <chr>      
    1 13.46.35.35

## Вывод

В ходе работы мы ознакомились с применением облачных технологий
хранения, подготовки и анализа данных, а также проанализировали
метаинформацию о сетевом трафике.
