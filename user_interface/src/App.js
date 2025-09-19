// App.js
import React, { useState, useEffect, useRef } from 'react';
import { Routes, Route, Link, useLocation } from 'react-router-dom';
import HomeComponent from './components/HomeComponent';
import SearchComponent from './components/SearchComponent';
import SearchResultsComponent from './components/SearchResultsComponent';
import AboutComponent from './components/AboutComponent';
import ContactComponent from './components/ContactComponent';
import SettingsComponent from './components/SettingsComponent';
import FullReportComponent from './components/FullReportComponent';
import DbCreatorComponent from './components/DbCreatorComponent';
import './App.css';

const App = () => {
  // State to control the dropdown menu
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef(null);
  const location = useLocation();

  // Toggle the mobile/desktop menu
  const toggleMenu = () => {
    setMenuOpen(!menuOpen);
  };

  // Close menu when clicking outside
  const handleClickOutside = (event) => {
    if (menuRef.current && !menuRef.current.contains(event.target)) {
      setMenuOpen(false);
    }
  };

  useEffect(() => {
    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);

  // Determine a CSS class based on the current route (for background styling, etc.)
  const getPageClass = () => {
    switch (location.pathname) {
      case '/about':
        return 'about';
      case '/contact':
        return 'contact';
      case '/results':
        return 'results';
      case '/settings':
        return 'settings';
      case '/search':
        return 'search';
      case '/create':
        return 'create';
      case '/':
        return 'home';
      default:
        return '';
    }
  };

  return (
    <div className="App">
      {/* Background image (adjust CSS for each page via getPageClass) */}
      <img
        src={`${process.env.PUBLIC_URL}/CORE_logo_no-words.png`}
        alt="Background"
        className={`background-image ${getPageClass()}`}
      />

      <header className="App-header">
        <Link to="/" className="home-icon">
          <img src={`${process.env.PUBLIC_URL}/blk_home.png`} alt="Home" />
        </Link>

        <div className="menu-icon">
          <img
            src={`${process.env.PUBLIC_URL}/menu.png`}
            alt="Menu"
            onClick={toggleMenu}
          />
        </div>

        <Link to="/settings" className="settings-icon">
          <img src={`${process.env.PUBLIC_URL}/gear1a.png`} alt="Settings" />
        </Link>

        {menuOpen && (
          <div className="dropdown-menu" ref={menuRef}>
            <Link to="/about" onClick={() => setMenuOpen(false)}>
              About
            </Link>
            <Link to="/contact" onClick={() => setMenuOpen(false)}>
              Contact/Help
            </Link>
          </div>
        )}

        <div className="App-title">
          <h1>SCOUT</h1>
        </div>

        <nav>
          <Link to="/search">Search</Link>
          <Link to="/create">Create</Link>
        </nav>
      </header>

      <div className="hidden-buffer"></div>

      <main className="App-main">
        <Routes>
          <Route path="/" element={<HomeComponent />} />
          <Route path="/search" element={<SearchComponent />} />
          <Route path="/results" element={<SearchResultsComponent />} />
          <Route path="/create" element={<DbCreatorComponent />} />
          <Route path="/about" element={<AboutComponent />} />
          <Route path="/contact" element={<ContactComponent />} />
          <Route path="/settings" element={<SettingsComponent />} />
          <Route path="/report/:reportId" element={<FullReportComponent />} />
        </Routes>
      </main>
    </div>
  );
};

export default App;
