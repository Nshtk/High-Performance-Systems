FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine AS build

WORKDIR /app
COPY ./Source ./Source
RUN dotnet restore ./Source/lab1.Core/lab1.Core.csproj
RUN dotnet build ./Source/lab1.Core/lab1.Core.csproj -c Release -o /app/output
CMD 

FROM mcr.microsoft.com/dotnet/runtime:8.0-alpine
WORKDIR /app
COPY --from=build /app/output .
COPY ./Data ./Data
ENTRYPOINT ["dotnet", "lab1.Core.dll"]