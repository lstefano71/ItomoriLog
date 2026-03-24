# ✅ **2. Avalonia Resource Dictionary**

Filename: `Themes/ItomoriLogTheme.axaml`

This file defines:

✅ Brand colors  
✅ Light/dark foreground/background brushes  
✅ SolidColorBrush resources  
✅ Text styles (Title, Subtitle, Body, Caption)  
✅ Severity chips (Info/Warn/Error/Debug)  
✅ Icon and accent brushes  
✅ Default Button, Panel, and Border styling

> Drop into your Avalonia app and merge via `App.axaml`:
>
> ```xml
> <Application.Styles>
>   <FluentTheme Mode="Dark"/>
>   <StyleInclude Source="avares://ItomoriLog/Styles/ItomoriLogTheme.axaml"/>
> </Application.Styles>
> ```

Here is the **full dictionary**:

```xml
<?xml version="1.0" encoding="utf-8"?>
<ResourceDictionary
    xmlns="https://github.com/avaloniaui"
    xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml">

  <!-- ============================= -->
  <!-- BRAND COLORS                  -->
  <!-- ============================= -->
  <Color x:Key="Itomori.SakuraPink">#FF6DAE</Color>
  <Color x:Key="Itomori.NeonCyan">#0EA5E9</Color>
  <Color x:Key="Itomori.Onyx">#111418</Color>
  <Color x:Key="Itomori.Slate">#1F2430</Color>
  <Color x:Key="Itomori.Mist">#D6E2F0</Color>
  <Color x:Key="Itomori.Grey">#9CA3AF</Color>

  <!-- Severity colors -->
  <Color x:Key="Itomori.Info">#38BDF8</Color>
  <Color x:Key="Itomori.Warn">#F59E0B</Color>
  <Color x:Key="Itomori.Error">#EF4444</Color>
  <Color x:Key="Itomori.Debug">#9CA3AF</Color>

  <!-- ============================= -->
  <!-- BRUSHES                       -->
  <!-- ============================= -->
  <SolidColorBrush x:Key="Brush.SakuraPink" Color="{StaticResource Itomori.SakuraPink}"/>
  <SolidColorBrush x:Key="Brush.NeonCyan" Color="{StaticResource Itomori.NeonCyan}"/>
  <SolidColorBrush x:Key="Brush.Onyx" Color="{StaticResource Itomori.Onyx}"/>
  <SolidColorBrush x:Key="Brush.Slate" Color="{StaticResource Itomori.Slate}"/>
  <SolidColorBrush x:Key="Brush.Mist" Color="{StaticResource Itomori.Mist}"/>
  <SolidColorBrush x:Key="Brush.Grey" Color="{StaticResource Itomori.Grey}"/>

  <SolidColorBrush x:Key="Brush.Info" Color="{StaticResource Itomori.Info}"/>
  <SolidColorBrush x:Key="Brush.Warn" Color="{StaticResource Itomori.Warn}"/>
  <SolidColorBrush x:Key="Brush.Error" Color="{StaticResource Itomori.Error}"/>
  <SolidColorBrush x:Key="Brush.Debug" Color="{StaticResource Itomori.Debug}"/>

  <!-- Background & foreground -->
  <SolidColorBrush x:Key="Itomori.Background" Color="{StaticResource Itomori.Onyx}"/>
  <SolidColorBrush x:Key="Itomori.Panel" Color="{StaticResource Itomori.Slate}"/>
  <SolidColorBrush x:Key="Itomori.Foreground" Color="{StaticResource Itomori.Mist}"/>
  <SolidColorBrush x:Key="Itomori.SubtleForeground" Color="{StaticResource Itomori.Grey}"/>

  <!-- Accent brushes -->
  <SolidColorBrush x:Key="Itomori.AccentPrimary" Color="{StaticResource Itomori.SakuraPink}"/>
  <SolidColorBrush x:Key="Itomori.AccentSecondary" Color="{StaticResource Itomori.NeonCyan}"/>

  <!-- ============================= -->
  <!-- TEXT STYLES                   -->
  <!-- ============================= -->
  <Style x:Key="Text.Title" TargetType="TextBlock">
    <Setter Property="FontSize" Value="28"/>
    <Setter Property="FontWeight" Value="SemiBold"/>
    <Setter Property="Foreground" Value="{StaticResource Itomori.Foreground}"/>
  </Style>

  <Style x:Key="Text.Subtitle" TargetType="TextBlock">
    <Setter Property="FontSize" Value="20"/>
    <Setter Property="FontWeight" Value="Medium"/>
    <Setter Property="Foreground" Value="{StaticResource Itomori.SubtleForeground}"/>
  </Style>

  <Style x:Key="Text.Body" TargetType="TextBlock">
    <Setter Property="FontSize" Value="14"/>
    <Setter Property="Foreground" Value="{StaticResource Itomori.Foreground}"/>
  </Style>

  <Style x:Key="Text.Caption" TargetType="TextBlock">
    <Setter Property="FontSize" Value="12"/>
    <Setter Property="Foreground" Value="{StaticResource Itomori.SubtleForeground}"/>
  </Style>

  <!-- ============================= -->
  <!-- BUTTONS / PANELS / BORDERS    -->
  <!-- ============================= -->
  <Style Selector="Button">
    <Setter Property="Background" Value="{StaticResource Brush.Slate}"/>
    <Setter Property="Foreground" Value="{StaticResource Itomori.Mist}"/>
    <Setter Property="BorderBrush" Value="{StaticResource Brush.SakuraPink}"/>
    <Setter Property="BorderThickness" Value="2"/>
    <Setter Property="Padding" Value="10,6"/>
    <Setter Property="CornerRadius" Value="6"/>
  </Style>

  <Style Selector="Button:pointerover">
    <Setter Property="Background" Value="{StaticResource Brush.NeonCyan}"/>
    <Setter Property="Foreground" Value="Black"/>
  </Style>

  <Style Selector="Border[Theme='Panel']">
    <Setter Property="Background" Value="{StaticResource Itomori.Panel}"/>
    <Setter Property="CornerRadius" Value="8"/>
    <Setter Property="Padding" Value="12"/>
  </Style>

</ResourceDictionary>
```

***

# ✅ **3. Optional: App Icon Variants (Let me know)**

I can generate:

✅ Transparent version (no background circle)  
✅ Solid dark version  
✅ Monochrome line icon  
✅ macOS `.icns` bundle  
✅ Windows `.ico` 16–256px  
✅ iOS/Android icon packs (if you ever want a mobile viewer)

